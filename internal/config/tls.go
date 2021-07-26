package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

// SetupTLSConfig constructs a new *tls.Config using our personal TLSConfig struct and certs
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	// construct tls config with CertFile and KeyFile
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile,
			cfg.KeyFile,
		)
		if err != nil {
			return nil, err
		}
		// read from CAFile
		if cfg.CAFile != "" {
			b, err := ioutil.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, err
			}
			// validate CAFile
			ca := x509.NewCertPool()
			ok := ca.AppendCertsFromPEM([]byte(b))
			if !ok {
				return nil, fmt.Errorf(
					"failed to parse root certificate: %q",
					cfg.CAFile,
				)
			}
			if cfg.Server {
				tlsConfig.ClientCAs = ca
				tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			} else {
				tlsConfig.RootCAs = ca
			}
			tlsConfig.ServerName = cfg.ServerAddress
		}
	}
	return tlsConfig, nil
}
