package config

import (
	"fmt"
	"io"
	"time"

	"k8s.io/apimachinery/pkg/labels"
)

type Config struct {
	MetricsAddr string
	Entries     []*Entry `yaml:"entries"`
}

type Entry struct {
	Interval      time.Duration   `yaml:"interval"`
	Namespace     string          `yaml:"namespace"`
	LabelSelector labels.Selector `yaml:"labelSelector"`
	NodeLabel     string          `yaml:"nodeLabel"`
	ResyncPeriod  time.Duration   `yaml:"resyncPeriod"`
}

func Read(io.Reader) (*Config, error) {
	c := &Config{} // FIXME: Actually read.
	c.SetDefaults()

	return c, c.Validate()
}

func (c *Config) SetDefaults() {
	if c.MetricsAddr == "" {
		c.MetricsAddr = ":8080"
	}

	for _, e := range c.Entries {
		if e.Interval == 0 {
			e.Interval = time.Minute
		}
	}
}

func (c *Config) Validate() error {
	// FIXME: Err if more than one entry has the same `nodeLabel`.

	for i, e := range c.Entries {
		err := e.Validate()
		if err != nil {
			return fmt.Errorf("entry #%d is misconfigured: %w", i, err)
		}
	}

	return nil
}

func (e *Entry) Validate() error {
	// FIXME: Err if any field is empty

	if e.Namespace == "" {
		return fmt.Errorf("namespace must be set")
	}

	if e.NodeLabel == "" {
		return fmt.Errorf("nodeLabel must be set")
	}

	return nil
}
