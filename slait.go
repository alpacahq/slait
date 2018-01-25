package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	_ "net/http/pprof"

	"github.com/alpacahq/slait/cache"
	"github.com/alpacahq/slait/rest"
	"github.com/alpacahq/slait/utils"
	. "github.com/alpacahq/slait/utils/log"

	"github.com/jasonlvhit/gocron"
)

var peers []string
var nodeId = 1
var join = false

func init() {
	configFlag := flag.String("config", "slait.yaml", "Slait YAML configuration file")
	printVersion := flag.Bool("version", false, "print version string and exits")
	flag.Parse()

	// set logging to output to standard error
	flag.Lookup("logtostderr").Value.Set("true")
	if configFlag != nil {
		data, err := ioutil.ReadFile(*configFlag)
		if err != nil {
			Log(FATAL, "Failed to read configuration file - Error: %v", err)
		}
		err = utils.ParseConfig(data)
		if err != nil {
			Log(FATAL, "Failed to parse configuration file - Error: %v", err)
		}
	} else {
		Log(FATAL, "No configuration file provided.")
	}

	sigChannel := make(chan os.Signal)
	go func() {
		for sig := range sigChannel {
			switch sig {
			case syscall.SIGUSR1:
				Log(INFO, "Dumping stack traces due to SIGUSR1 request")
				pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			}
		}
	}()
	signal.Notify(sigChannel, syscall.SIGUSR1)

	if *printVersion {
		fmt.Printf("Slait version %s (%s)\n", utils.Version, utils.Sha1hash)
		os.Exit(0)
	}
}

func main() {
	Log(INFO, "Launching Slait.")

	cache.Build(utils.GlobalConfig.DataDir)
	cache.Fill()

	gocron.Every(1).Minute().Do(cache.Trim)
	go func() { <-gocron.Start() }()

	// Start REST API
	restApi := rest.REST{Port: utils.GlobalConfig.ListenPort}
	Log(INFO, "Starting REST & websocket server on port %v", restApi.Port)
	if err := restApi.Start(); err != nil {
		Log(FATAL, "Failed to start server - Error: %v", err)
	}
}

func shutdown() {
	Log(INFO, "Exiting...")
	os.Exit(0)
}
