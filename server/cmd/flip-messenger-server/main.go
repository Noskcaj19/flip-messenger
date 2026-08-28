package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Noskcaj19/flip-messenger/server"
)

func main() {
	configPath := flag.String("config", "config.json", "path to the server configuration")
	flag.Parse()

	config, err := flipmessenger.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if !config.AllowHTTP && (config.TLSCert == "" || config.TLSKey == "") {
		log.Fatal("tls_cert and tls_key are required")
	}
	app, err := flipmessenger.New(config)
	if err != nil {
		log.Fatal(err)
	}
	if err := app.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	httpServer := &http.Server{
		Addr:              config.Listen,
		Handler:           app.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}()

	if config.AllowHTTP {
		log.Printf("WARNING: Flip Messenger server using unencrypted HTTP on %s", config.Listen)
		err = httpServer.ListenAndServe()
	} else {
		log.Printf("Flip Messenger server listening on https://%s", config.Listen)
		err = httpServer.ListenAndServeTLS(config.TLSCert, config.TLSKey)
	}
	if err != nil && err != http.ErrServerClosed {
		log.Fatal(fmt.Errorf("serve: %w", err))
	}
}
