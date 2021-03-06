package main

import (
	"os"

	"gopkg.in/src-d/go-mysql-server.v0/auth"

	"github.com/go-toschool/jsonql"
	"github.com/sirupsen/logrus"
	sql "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/server"
)

func main() {
	dir := "."
	if len(os.Args) >= 2 {
		dir = os.Args[1]
	}
	engine := sql.NewDefault()
	d, err := jsonql.NewDatabase("logs", dir)
	if err != nil {
		logrus.Fatalf("could not create database: %v", err)
	}
	engine.AddDatabase(d)

	if err := engine.Init(); err != nil {
		logrus.Fatalf("could not initialize server: %v", err)
	}

	cfg := server.Config{
		Protocol: "tcp",
		Address:  "127.0.0.1:3306",
		Auth:     auth.NewNativeSingle("user", "pass", auth.AllPermissions),
	}
	s, err := server.NewDefaultServer(cfg, engine)
	if err != nil {
		logrus.Fatalf("could not create default server: %v", err)
	}

	logrus.Infof("server started on %s", cfg.Address)
	if err := s.Start(); err != nil {
		logrus.Fatalf("server failed: %v", err)
	}
}
