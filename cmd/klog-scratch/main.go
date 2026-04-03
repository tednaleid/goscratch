package main

import (
	"flag"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)

	klog.Info("This will print with headers")

	flag.Set("skip_headers", "true")
	flag.Parse()

	klog.Info("This will print without headers")
}
