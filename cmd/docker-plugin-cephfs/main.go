package main

import (
	"log"

	"docker-plugin-cephfs/pkg/volume"
	plugin "github.com/docker/go-plugins-helpers/volume"
)

const socket = "cephfs"

func main() {
	driver := volume.NewDriver()
	handler := plugin.NewHandler(driver)
	log.Fatal(handler.ServeUnix(socket, 1))
}
