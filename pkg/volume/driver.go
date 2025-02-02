// driver.go
package volume

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/ini.v1"

	plugin "github.com/docker/go-plugins-helpers/volume"

	_ "github.com/mattn/go-sqlite3"
)

// NodeInfo stores the last heartbeat timestamp and the external hostname.
type NodeInfo struct {
	Timestamp int64  `json:"timestamp"`
	Hostname  string `json:"hostname"`
}

// Volume represents a volume's metadata.
type Volume struct {
	Name        string              `json:"name"`
	CreatedAt   string              `json:"created_at"`
	ActiveNodes map[string]NodeInfo `json:"active_nodes,omitempty"`
}

// Driver is the main driver.
type Driver struct {
	ConfigPath string
	// MountPath is the persistent mount location, passed in via VOLUME_MOUNT_ROOT.
	MountPath string
	Servers   []string
	// RemotePrefix for CephFS.
	RemotePrefix string
	DebugMode    bool

	// Preserved CephFS mounting configuration.
	ClientName  string
	ClusterName string

	DB *sql.DB
	// secretCache caches the secret keyed by client name.
	secretCache map[string]string

	// localMounts keeps track of mounts (per volume) initiated by this node.
	// Map: volume name -> set of reqIDs (represented as map[string]bool)
	localMounts map[string]map[string]bool

	// GlobalScope is a configurable field determining whether the driver
	// advertises global or local scope. It is set via the GLOBAL_SCOPE env var.
	GlobalScope bool

	// Interfaces for mounting the CephFS share and managing directories.
	mnt Mounter
	dir DirectoryMaker

	sync.RWMutex
}

// TableRow represents a row in the ASCII table
type TableRow struct {
	VolumeName string
	CreatedAt  string
	ReqInfo    []string // Each string is a complete req info line
}

// generateASCIITable creates an ASCII table representation of volume data
func generateASCIITable(rows []TableRow) string {
	// Find the maximum widths for each column
	nameWidth := 11 // minimum "Volume Name" width
	timeWidth := 20 // minimum "Created At" width
	reqWidth := 48  // minimum "Active Reqs" width

	for _, row := range rows {
		if len(row.VolumeName) > nameWidth {
			nameWidth = len(row.VolumeName)
		}
		if len(row.CreatedAt) > timeWidth {
			timeWidth = len(row.CreatedAt)
		}
		for _, req := range row.ReqInfo {
			if len(req) > reqWidth {
				reqWidth = len(req)
			}
		}
	}

	// Build the header
	sep := fmt.Sprintf("+-%s-+-%s-+-%s-+\n",
		strings.Repeat("-", nameWidth),
		strings.Repeat("-", timeWidth),
		strings.Repeat("-", reqWidth))

	header := fmt.Sprintf("| %-*s | %-*s | %-*s |\n",
		nameWidth, "Volume Name",
		timeWidth, "Created At",
		reqWidth, "Active Reqs")

	var builder strings.Builder
	builder.WriteString(sep)
	builder.WriteString(header)
	builder.WriteString(sep)

	// Build the rows
	for _, row := range rows {
		if len(row.ReqInfo) == 0 {
			// Single row with no requests
			builder.WriteString(fmt.Sprintf("| %-*s | %-*s | %-*s |\n",
				nameWidth, row.VolumeName,
				timeWidth, row.CreatedAt,
				reqWidth, ""))
		} else {
			// First line with volume name and creation time
			builder.WriteString(fmt.Sprintf("| %-*s | %-*s | %-*s |\n",
				nameWidth, row.VolumeName,
				timeWidth, row.CreatedAt,
				reqWidth, row.ReqInfo[0]))

			// Additional lines for more requests
			for _, req := range row.ReqInfo[1:] {
				builder.WriteString(fmt.Sprintf("| %-*s | %-*s | %-*s |\n",
					nameWidth, "",
					timeWidth, "",
					reqWidth, req))
			}
		}
		builder.WriteString(sep)
	}

	return builder.String()
}

// writeVolumeTable generates and writes the volume table to a file atomically
func (d *Driver) writeVolumeTable() error {
	hostname, err := getExternalHostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %v", err)
	}

	// Create a temporary file with hostname
	tempFile := path.Join(d.MountPath, fmt.Sprintf(".%s.tmp", hostname))
	finalFile := path.Join(d.MountPath, "current.txt")

	// Get all volumes
	rows, err := d.DB.Query("SELECT name, created_at, active_nodes FROM volumes")
	if err != nil {
		return fmt.Errorf("query error: %v", err)
	}
	defer rows.Close()

	var tableRows []TableRow
	for rows.Next() {
		var name, createdAt, activeNodesStr string
		if err := rows.Scan(&name, &createdAt, &activeNodesStr); err != nil {
			return fmt.Errorf("scan error: %v", err)
		}

		row := TableRow{
			VolumeName: name,
			CreatedAt:  createdAt,
		}

		// Parse active nodes
		var activeNodes map[string]NodeInfo
		if activeNodesStr != "" {
			if err := json.Unmarshal([]byte(activeNodesStr), &activeNodes); err != nil {
				return fmt.Errorf("unmarshal error: %v", err)
			}

			// Convert node info to sorted strings
			var reqInfos []string
			for reqID, info := range activeNodes {
				// Format timestamp
				ts := time.Unix(info.Timestamp, 0).UTC().Format("2006-01-02 15:04:05")
				// Truncate reqID to 15 chars
				if len(reqID) > 15 {
					reqID = reqID[:15]
				}
				// Format: timestamp | reqID | hostname
				reqInfos = append(reqInfos, fmt.Sprintf("%s | %s | %s", ts, reqID, info.Hostname))
			}

			// Sort by timestamp
			sort.Strings(reqInfos)
			row.ReqInfo = reqInfos
		}

		tableRows = append(tableRows, row)
	}

	// Sort rows by volume name
	sort.Slice(tableRows, func(i, j int) bool {
		return tableRows[i].VolumeName < tableRows[j].VolumeName
	})

	// Generate the table
	table := generateASCIITable(tableRows)

	// Write to temp file
	if err := os.WriteFile(tempFile, []byte(table), 0644); err != nil {
		return fmt.Errorf("write error: %v", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, finalFile); err != nil {
		os.Remove(tempFile) // Clean up temp file
		return fmt.Errorf("rename error: %v", err)
	}

	return nil
}

// Mounter is an interface for performing mount and unmount operations.
type Mounter interface {
	Mount(source, target, fstype, options string) error
	Unmount(target string) error
}

// DirectoryMaker is an interface to check and create directories.
type DirectoryMaker interface {
	IsDir(dir string) bool
	MakeDir(dir string, mode os.FileMode) error
	MakeTempDir() (string, error)
}

const (
	defaultConfigPath   = "/etc/ceph/"
	dbFilename          = "volumedb.sqlite"
	remotePrefixDefault = ""
	// Timeout in seconds after which a heartbeat is considered stale.
	staleTimeoutSeconds = 120
	// Interval for cleanup routine.
	cleanupInterval = 30 * time.Second
	// Interval for refreshing local heartbeats.
	refreshInterval = 30 * time.Second
)

// isValidVolumeName checks if the volume name is safe from path traversal
func isValidVolumeName(name string) bool {
	// Clean the path and check if it tries to escape
	cleaned := path.Clean(name)

	// Reject if:
	// 1. Name contains .. sequences
	// 2. Name starts with /
	// 3. Cleaned path is different from original (indicates attempted traversal)
	return !strings.Contains(cleaned, "..") &&
		!strings.HasPrefix(cleaned, "/") &&
		cleaned == name
}

// validateVolumePath ensures a volume path is within the mount root
func validateVolumePath(mountRoot, volName string) error {
	if !isValidVolumeName(volName) {
		return fmt.Errorf("invalid volume name %q may be attempting path traversal", volName)
	}

	intended := path.Join(mountRoot, volName)
	// Double-check the final path
	if !strings.HasPrefix(path.Clean(intended), path.Clean(mountRoot)) {
		return fmt.Errorf("volume path %q escapes mount root", intended)
	}

	return nil
}

// initDB creates the volumes table if it does not exist.
func initDB(db *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS volumes (
		name TEXT PRIMARY KEY,
		created_at TEXT,
		active_nodes TEXT
	);`
	_, err := db.Exec(query)
	return err
}

// fetchVol retrieves volume metadata from the SQLite database.
func (d *Driver) fetchVol(name string) (*Volume, error) {
	row := d.DB.QueryRow("SELECT name, created_at, active_nodes FROM volumes WHERE name = ?", name)
	var vol Volume
	var activeNodesStr string
	err := row.Scan(&vol.Name, &vol.CreatedAt, &activeNodesStr)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(activeNodesStr), &vol.ActiveNodes); err != nil {
		vol.ActiveNodes = make(map[string]NodeInfo)
	}
	return &vol, nil
}

// saveVol writes volume metadata into the SQLite database.
func (d *Driver) saveVol(name string, vol Volume) error {
	activeNodesJSON, err := json.Marshal(vol.ActiveNodes)
	if err != nil {
		return fmt.Errorf("serialization error: %v", err)
	}
	_, err = d.DB.Exec(`INSERT OR REPLACE INTO volumes (name, created_at, active_nodes)
		VALUES (?, ?, ?)`, name, vol.CreatedAt, string(activeNodesJSON))
	return err
}

// getExternalHostname reads the host's external hostname from /etc/hostname-external.
func getExternalHostname() (string, error) {
	data, err := os.ReadFile("/etc/hostname-external")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// secret loads and caches the secret from the keyring using the Driver's configuration.
func (d *Driver) secret() (string, error) {
	if d.secretCache == nil {
		d.secretCache = make(map[string]string)
	}
	if sec, ok := d.secretCache[d.ClientName]; ok {
		return sec, nil
	}
	keyringPath := fmt.Sprintf("%s/%s.client.%s.keyring", strings.TrimRight(d.ConfigPath, "/"), d.ClusterName, d.ClientName)
	log.Printf("Loading keyring from path: %s", keyringPath)
	cnf, err := ini.Load(keyringPath)
	if err != nil {
		return "", fmt.Errorf("failed to load keyring %s: %v", keyringPath, err)
	}
	secSection, err := cnf.GetSection("client." + d.ClientName)
	if err != nil {
		return "", fmt.Errorf("keyring missing client details for %s", d.ClientName)
	}
	key := secSection.Key("key").String()
	d.secretCache[d.ClientName] = key
	return key, nil
}

// ensurePersistentMount verifies that the CephFS share is mounted at MountPath;
// if not, it attempts to mount it.
func (d *Driver) ensurePersistentMount() error {
	info, err := os.Stat(d.MountPath)
	if err != nil || !info.IsDir() {
		log.Printf("MountPath %s not available; attempting remount...", d.MountPath)
		secret, err := d.secret()
		if err != nil {
			return fmt.Errorf("failed to retrieve secret: %v", err)
		}
		opts := fmt.Sprintf("name=%s,secret=%s", d.ClientName, secret)
		if extra := EnvOrDefault("MOUNT_OPTS", ""); extra != "" {
			opts = opts + "," + extra
		}
		connStr := CephConnStr(d.Servers, d.RemotePrefix)
		if err = d.mnt.Mount(connStr, d.MountPath, "ceph", opts); err != nil {
			return fmt.Errorf("failed to mount CephFS at %s: %v", d.MountPath, err)
		}
		log.Printf("CephFS mounted persistently at %s", d.MountPath)
	}
	return nil
}

// Create creates a new volume.
func (d *Driver) Create(req *plugin.CreateRequest) error {
	if err := validateVolumePath(d.MountPath, req.Name); err != nil {
		return err
	}
	d.Lock()
	defer d.Unlock()

	if d.DebugMode {
		log.Printf("Create request: %+v", req)
	}

	_, err := d.fetchVol(req.Name)
	if err == nil {
		return fmt.Errorf("volume %s already exists", req.Name)
	} else if err != sql.ErrNoRows && !strings.Contains(err.Error(), "no rows") {
		return fmt.Errorf("error checking volume existence: %v", err)
	}

	vol := Volume{
		Name:        req.Name,
		CreatedAt:   time.Now().Format(time.RFC3339),
		ActiveNodes: make(map[string]NodeInfo),
	}

	if err := d.ensurePersistentMount(); err != nil {
		return fmt.Errorf("mount error: %v", err)
	}

	volDir := path.Join(d.MountPath, req.Name)
	if !d.dir.IsDir(volDir) {
		if err := d.dir.MakeDir(volDir, 0755); err != nil {
			return fmt.Errorf("unable to create volume directory %s: %v", volDir, err)
		}
	}

	if err := d.saveVol(req.Name, vol); err != nil {
		return fmt.Errorf("failed to save volume metadata: %v", err)
	}

	defer func() {
		if err := d.writeVolumeTable(); err != nil {
			log.Printf("Warning: failed to update volume table: %v", err)
		}
	}()

	return nil
}

// List returns all volumes.
func (d *Driver) List() (*plugin.ListResponse, error) {
	d.RLock()
	defer d.RUnlock()

	rows, err := d.DB.Query("SELECT name, created_at, active_nodes FROM volumes")
	if err != nil {
		return nil, fmt.Errorf("query error: %v", err)
	}
	defer rows.Close()

	var vols []*plugin.Volume
	for rows.Next() {
		var name, createdAt, activeNodesStr string
		if err := rows.Scan(&name, &createdAt, &activeNodesStr); err != nil {
			return nil, fmt.Errorf("scan error: %v", err)
		}
		vols = append(vols, &plugin.Volume{
			Name:       name,
			Mountpoint: path.Join(d.MountPath, name),
			CreatedAt:  createdAt,
			Status: map[string]interface{}{
				"active_nodes": activeNodesStr,
			},
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %v", err)
	}

	return &plugin.ListResponse{Volumes: vols}, nil
}

// Get returns a volume.
func (d *Driver) Get(req *plugin.GetRequest) (*plugin.GetResponse, error) {
	if err := validateVolumePath(d.MountPath, req.Name); err != nil {
		return nil, err
	}
	d.RLock()
	defer d.RUnlock()

	if d.DebugMode {
		log.Printf("Get request: %+v", req)
	}

	vol, err := d.fetchVol(req.Name)
	if err != nil {
		return nil, fmt.Errorf("DB read error: %v", err)
	}

	return &plugin.GetResponse{
		Volume: &plugin.Volume{
			Name:       vol.Name,
			Mountpoint: path.Join(d.MountPath, vol.Name),
			CreatedAt:  vol.CreatedAt,
			Status:     map[string]interface{}{"active_nodes": vol.ActiveNodes},
		},
	}, nil
}

// Remove deletes a volume.
func (d *Driver) Remove(req *plugin.RemoveRequest) error {
	d.Lock()
	defer d.Unlock()

	if d.DebugMode {
		log.Printf("Remove request: %+v", req)
	}

	vol, err := d.fetchVol(req.Name)
	if err != nil {
		return fmt.Errorf("DB read error: %v", err)
	}

	if len(vol.ActiveNodes) > 0 {
		return fmt.Errorf("volume is still in use")
	}

	_, err = d.DB.Exec("DELETE FROM volumes WHERE name = ?", req.Name)
	if err != nil {
		return fmt.Errorf("DB delete error: %v", err)
	}

	return nil
}

// Path returns the persistent volume directory.
func (d *Driver) Path(req *plugin.PathRequest) (*plugin.PathResponse, error) {
	d.RLock()
	defer d.RUnlock()

	if d.DebugMode {
		log.Printf("Path request: %+v", req)
	}

	return &plugin.PathResponse{Mountpoint: path.Join(d.MountPath, req.Name)}, nil
}

// Mount updates usage tracking for the volume; no actual bind mount is performed.
func (d *Driver) Mount(req *plugin.MountRequest) (*plugin.MountResponse, error) {
	if err := validateVolumePath(d.MountPath, req.Name); err != nil {
		return nil, err
	}
	// Get current data without lock
	vol, err := d.fetchVol(req.Name)
	if err != nil {
		return nil, fmt.Errorf("DB read error: %v", err)
	}

	// Prepare the data we want to write
	hostname, err := getExternalHostname()
	if err != nil {
		hostname, _ = os.Hostname()
	}

	// Now take the lock only for updating memory state
	d.Lock()
	if vol.ActiveNodes == nil {
		vol.ActiveNodes = make(map[string]NodeInfo)
	}
	vol.ActiveNodes[req.ID] = NodeInfo{
		Timestamp: time.Now().Unix(),
		Hostname:  hostname,
	}

	// Update local tracking
	if d.localMounts == nil {
		d.localMounts = make(map[string]map[string]bool)
	}
	if _, ok := d.localMounts[req.Name]; !ok {
		d.localMounts[req.Name] = make(map[string]bool)
	}
	d.localMounts[req.Name][req.ID] = true
	d.Unlock()

	// Do the database write after releasing the lock
	if err := d.saveVol(req.Name, *vol); err != nil {
		// If save fails, we should try to rollback our memory state
		d.Lock()
		delete(d.localMounts[req.Name], req.ID)
		if len(d.localMounts[req.Name]) == 0 {
			delete(d.localMounts, req.Name)
		}
		d.Unlock()
		return nil, fmt.Errorf("DB update error: %v", err)
	}

	// Defer table generation until after mount completes
	defer func() {
		if err := d.writeVolumeTable(); err != nil {
			log.Printf("Warning: failed to update volume table: %v", err)
		}
	}()

	return &plugin.MountResponse{Mountpoint: path.Join(d.MountPath, req.Name)}, nil
}

// Unmount removes usage tracking for the volume.
func (d *Driver) Unmount(req *plugin.UnmountRequest) error {
	d.Lock()
	defer d.Unlock()

	if d.DebugMode {
		log.Printf("Unmount request: %+v", req)
	}

	vol, err := d.fetchVol(req.Name)
	if err != nil {
		return fmt.Errorf("DB read error: %v", err)
	}

	if vol.ActiveNodes != nil {
		delete(vol.ActiveNodes, req.ID)
	}
	// Remove from localMounts if present.
	if d.localMounts != nil {
		if mounts, ok := d.localMounts[req.Name]; ok {
			delete(mounts, req.ID)
			if len(mounts) == 0 {
				delete(d.localMounts, req.Name)
			}
		}
	}

	if err := d.saveVol(req.Name, *vol); err != nil {
		return fmt.Errorf("DB update error: %v", err)
	}

	// Defer table generation until after mount completes
	defer func() {
		if err := d.writeVolumeTable(); err != nil {
			log.Printf("Warning: failed to update volume table: %v", err)
		}
	}()

	return nil
}

// Capabilities returns the driver's capabilities.
// It now returns a scope based on the GlobalScope field.
func (d *Driver) Capabilities() *plugin.CapabilitiesResponse {
	scope := "global"
	if !d.GlobalScope {
		scope = "local"
	}
	return &plugin.CapabilitiesResponse{
		Capabilities: plugin.Capability{Scope: scope},
	}
}

// refreshLocalHeartbeats refreshes the on-disk heartbeat timestamps for all local mounts.
// It iterates over the in-memory localMounts map and, for each recorded reqID, updates its timestamp
// and hostname in the corresponding volume's ActiveNodes record.
func (d *Driver) refreshLocalHeartbeats() {
	d.Lock()
	defer d.Unlock()

	now := time.Now().Unix()
	hostname, err := getExternalHostname()
	if err != nil {
		hostname, _ = os.Hostname()
	}

	for volName, reqIDs := range d.localMounts {
		vol, err := d.fetchVol(volName)
		if err != nil {
			log.Printf("Heartbeat refresh: could not fetch volume %s: %v", volName, err)
			continue
		}
		updated := false
		if vol.ActiveNodes == nil {
			vol.ActiveNodes = make(map[string]NodeInfo)
		}
		for reqID := range reqIDs {
			if info, exists := vol.ActiveNodes[reqID]; exists {
				info.Timestamp = now
				info.Hostname = hostname
				vol.ActiveNodes[reqID] = info
				updated = true
			}
		}
		if updated {
			if err := d.saveVol(volName, *vol); err != nil {
				log.Printf("Heartbeat refresh: failed to update volume %s: %v", volName, err)
			}
		}
	}
	// After all updates, generate new table
	if err := d.writeVolumeTable(); err != nil {
		log.Printf("Warning: failed to update volume table during heartbeat refresh: %v", err)
	}
}

// CleanupStaleNodes scans all volumes and removes node entries with heartbeats older than the timeout.
func (d *Driver) CleanupStaleNodes(timeout int64) {
	// First get the list of volumes without holding the lock for too long
	var volumes []struct {
		name           string
		activeNodesStr string
	}

	d.RLock() // Use read lock for querying
	rows, err := d.DB.Query("SELECT name, active_nodes FROM volumes")
	if err != nil {
		log.Printf("Cleanup DB query error: %v", err)
		d.RUnlock()
		return
	}

	for rows.Next() {
		var vol struct {
			name           string
			activeNodesStr string
		}
		if err := rows.Scan(&vol.name, &vol.activeNodesStr); err != nil {
			log.Printf("Cleanup scan error: %v", err)
			continue
		}
		volumes = append(volumes, vol)
	}
	rows.Close()
	d.RUnlock()

	now := time.Now().Unix()

	// Process each volume independently
	for _, vol := range volumes {
		var activeNodes map[string]NodeInfo
		if err := json.Unmarshal([]byte(vol.activeNodesStr), &activeNodes); err != nil {
			log.Printf("JSON unmarshal error for volume %s: %v", vol.name, err)
			continue
		}

		changed := false
		for nodeID, info := range activeNodes {
			if now-info.Timestamp > timeout {
				log.Printf("Removing stale heartbeat: volume=%s, reqID=%s, last_update=%d",
					vol.name, nodeID, info.Timestamp)
				delete(activeNodes, nodeID)
				changed = true
			}
		}

		if changed {
			activeNodesJSON, err := json.Marshal(activeNodes)
			if err != nil {
				log.Printf("Cleanup marshal error for volume %s: %v", vol.name, err)
				continue
			}

			// Only lock when we need to update
			d.Lock()
			_, err = d.DB.Exec("UPDATE volumes SET active_nodes = ? WHERE name = ?",
				string(activeNodesJSON), vol.name)
			if err != nil {
				log.Printf("Cleanup DB update error for volume %s: %v", vol.name, err)
			}
			d.Unlock()
		}
	}

	// After all updates, generate new table
	if err := d.writeVolumeTable(); err != nil {
		log.Printf("Warning: failed to update volume table during heartbeat refresh: %v", err)
	}
}

// NewDriver initializes the driver using VOLUME_MOUNT_ROOT as the mount location,
// preserves the existing CephFS mounting configuration, mounts the CephFS share,
// opens the shared SQLite database with a busy timeout for cross-node concurrency,
// resets volume usage state, and starts background goroutines for cleanup and heartbeat refresh.
func NewDriver() *Driver {
	servers := strings.Split(EnvOrDefault("SERVERS", "localhost"), ",")
	remotePrefix := EnvOrDefault("REMOTE_PREFIX", remotePrefixDefault)
	debugMode := EnvOrDefaultBool("DEBUG_MODE", false)
	mountRoot := EnvOrDefault("VOLUME_MOUNT_ROOT", plugin.DefaultDockerRootDirectory)

	driver := &Driver{
		ConfigPath:   EnvOrDefault("CONFIG_PATH", defaultConfigPath),
		MountPath:    mountRoot,
		Servers:      servers,
		RemotePrefix: remotePrefix,
		DebugMode:    debugMode,
		ClientName:   EnvOrDefault("CLIENT_NAME", "admin"),
		ClusterName:  EnvOrDefault("CLUSTER_NAME", "ceph"),
		// Set GlobalScope based on the GLOBAL_SCOPE env var (defaults to true)
		GlobalScope: EnvOrDefaultBool("GLOBAL_SCOPE", true),
		mnt:         fsMounter{},
		dir:         osDirectoryMaker{},
		localMounts: make(map[string]map[string]bool),
	}

	log.Printf("Driver configuration: %+v", driver)

	// Mount the CephFS share persistently at MountPath.
	secret, err := driver.secret()
	if err != nil {
		log.Fatalf("Secret retrieval failed: %v", err)
	}
	opts := fmt.Sprintf("name=%s,secret=%s", driver.ClientName, secret)
	if extraOpts := EnvOrDefault("MOUNT_OPTS", ""); extraOpts != "" {
		opts = opts + "," + extraOpts
	}
	connStr := CephConnStr(driver.Servers, driver.RemotePrefix)
	if err := driver.mnt.Mount(connStr, driver.MountPath, "ceph", opts); err != nil {
		log.Fatalf("Could not mount CephFS persistently at %s: %v", driver.MountPath, err)
	}
	if driver.DebugMode {
		log.Printf("CephFS mounted persistently at %s", driver.MountPath)
	}

	// Open the SQLite database with a busy timeout (5 seconds) for cross-node concurrency.
	dbPath := path.Join(driver.MountPath, dbFilename)
	dsn := fmt.Sprintf("%s?_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatalf("Could not open SQLite DB at %s: %v", dbPath, err)
	}
	if err := initDB(db); err != nil {
		log.Fatalf("SQLite DB initialization failed: %v", err)
	}
	driver.DB = db

	// Start background goroutine for cleanup of stale heartbeats.
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		for range ticker.C {
			driver.CleanupStaleNodes(staleTimeoutSeconds)
		}
	}()

	// Start background goroutine to refresh local heartbeats every refreshInterval.
	go func() {
		ticker := time.NewTicker(refreshInterval)
		for range ticker.C {
			driver.refreshLocalHeartbeats()
		}
	}()

	return driver
}
