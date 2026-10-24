// TLS test configuration: fixtures supplied explicitly, no trust bypass.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

func fixtureTLS(server bool, identity string) (*tls.Config, error) {
	root := os.Getenv("MCP_RS_TLS_FIXTURES")
	if root == "" {
		return nil, fmt.Errorf("MCP_RS_TLS_FIXTURES is required")
	}
	ca := "ca.pem"
	if identity == "wrong-ca" {
		ca = "untrusted-ca.pem"
	}
	pem, err := os.ReadFile(filepath.Join(root, ca))
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("invalid fixture CA")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "localhost"}
	if identity == "wrong-host" {
		config.ServerName = "wrong.test"
	}
	name := ""
	if server {
		name = "server"
	} else if identity == "trusted" {
		name = "client"
	} else if identity == "untrusted" {
		name = "untrusted-client"
	}
	if name != "" {
		pair, err := tls.LoadX509KeyPair(filepath.Join(root, name+".pem"), filepath.Join(root, name+"-key.pem"))
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{pair}
	}
	if server && identity == "mtls" {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = roots
	}
	return config, nil
}
