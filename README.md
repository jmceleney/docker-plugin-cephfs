# Docker Volume Plugin for CephFS
This is based on the work of @Brindster. He wanted a docker volume plugin that didn't require
the secrets to be passed to the driver, but instead one that could read from keyring
files instead.

My requirements went a few steps further:
1. The driver now take a REMOTE_PREFIX variable in order to allow for cephfs subvolumes.  
It is now possible to have all volumes as a sudsirectory of the specified path.
1. The original scope was "local" which implied that the created volumes were host specific.  
I wanted to be able to define cluster-wide volumes. There is now a boolean GLOBAL_SCOPE Env  
variable that defaults to true.
1. Metadata is now stored on the cephfs volume itself. This was necessary for Global Scope.  
As part of this the database is now SQLLite, as BoltDB was not safe to use on Cephfs due  
to locking issues.
1. The driver daemons now update a file "current.txt" at the root of the ceph filesystem/subvolume.  
This is an ascii-table dump of the data in the SQLLite database.
1. Instead of making many independent cephfs mounts (one per volume) the driver makes a single  
cephfs mount per node, and then returns the path to the subdirectory containing the requested  
volume.

## Example current.txt

### Suggested use
```bash
root@swarm1:/mnt/docker_volumes# ln -s /var/lib/docker/plugins/`docker plugin inspect cephfs --format '{{.Id}}'`/propagated-mount/current.txt ~/
root@swarm1:/mnt/docker_volumes# cat ~/current.txt
```
```
+-------------+----------------------+--------------------------------------------------+
| Volume Name | Created At           | Active Reqs                                      |
+-------------+----------------------+--------------------------------------------------+
| my_volume_a | 2025-02-02T22:44:38Z |                                                  |
+-------------+----------------------+--------------------------------------------------+
| my_volume_b | 2025-02-02T22:44:42Z |                                                  |
+-------------+----------------------+--------------------------------------------------+
| testvol     | 2025-02-02T19:06:02Z | 2025-02-02 22:54:17 | 0915aba5db5f5d9 | swarm1   |
|             |                      | 2025-02-02 22:54:17 | 304c578eca7f20d | swarm3   |
+-------------+----------------------+--------------------------------------------------+
| testvol2    | 2025-02-02T19:10:02Z | 2025-02-02 22:54:17 | 9fe5850cdea4e25 | swarm2   |
+-------------+----------------------+--------------------------------------------------+
```
```bash
root@swarm1:/mnt/docker_volumes# watch cat ~/current.txt

```

## Requirements
Since this plugin reads from your keyring files, it requires the folder `/etc/ceph` to
exist and be readable by the docker user. Keyring files should be stored in this folder
in order to be discoverable by the plugin.

Keyring files are expected to follow the naming pattern `ceph.client.admin.keyring` by 
default where `ceph` is the name of the cluster and `admin` is the name of the client.

## Ceph client creation
To create a client on your ceph cluster, you can run a command similar to the following:
```shell script
ceph auth get-or-create client.dockeruser mon 'allow r' osd 'allow rw' mds 'allow' \
> /etc/ceph/ceph.client.dockeruser.keyring
```
Then, copy over this keyring file to your docker hosts.

## Installation (root of cephfs filesystem)
```shell script
docker plugin install --alias cephfs jmceleney/docker-plugin-cephfs \
CLUSTER_NAME=ceph \
CLIENT_NAME=admin \
SERVERS=ceph1,ceph2,ceph3
```

## Installation (subvolume of cephfs filesystem)
```shell script
docker plugin install --alias cephfs jmceleney/docker-plugin-cephfs \
CLUSTER_NAME=ceph \
CLIENT_NAME=admin \
SERVERS=ceph1,ceph2,ceph3 \
REMOTE_PREFIX=/path/to/subvolume
```

There are three settings that can be modified on the plugin during installation. These
settings act as default values, all of them are overridable when creating volumes.
* *CLUSTER_NAME* is the default name of the cluster. Defaults to _ceph_.
* *CLIENT_NAME* is the default name of the client. Defaults to _admin_.
* *SERVERS* is a comma-delimited list of ceph monitors to connect to. Defaults to _localhost_.
* *REMOTE_PREFIX* The path to the subvolume within the cephfs filesystem (optional)
* *GLOBAL_SCOPE* The driver is cluster global by default. Set this to "0" to declare  local scope.

Also, debug mode can be enabled on the plugin to output verbose logs during plugin operation.
Debug mode is enabled using the `DEBUG_MODE=1` value.

## Usage
Create a volume directly from the command line:
```shell script
docker volume create --driver cephfs test
docker run -it --rm -v test:/data busybox sh
```

Alternatively, use from a docker-compose file:
```yaml
version: '3'

services:
  app:
    image: nginx
    volumes:
      - test:/data

volumes:
    test:
        driver: cephfs
```

## Driver options
Unlike the driver that this is based on, driver_opts are ignored.