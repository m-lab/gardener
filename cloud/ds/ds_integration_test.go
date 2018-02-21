// +build integration

package ds_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/m-lab/etl-gardener/cloud/ds"
	"google.golang.org/api/option"
)

func Options() []option.ClientOption {
	opts := []option.ClientOption{}
	if os.Getenv("TRAVIS") != "" {
		authOpt := option.WithCredentialsFile("../../travis-testing.key")
		opts = append(opts, authOpt)
	}

	return opts
}

type TestObj struct {
	Name  string
	Count int
}

// TODO use DATASTORE_EMULATOR_HOST
func TestSaveLoad(t *testing.T) {
	os.Setenv("PROJECT", "mlab-testing")
	saver, err := ds.NewSaver("", "test", Options()...)
	if err != nil {
		t.Fatal(err)
	}
	obj := TestObj{"foobar", 123}
	err = saver.Save("test2", &obj)
	if err != nil {
		t.Fatal(err)
	}

	var ld TestObj
	err = saver.Load("test2", &ld)
	if err != nil {
		t.Fatal(err)
	}
	if ld.Name != obj.Name || ld.Count != obj.Count {
		t.Error(fmt.Sprintf("Not matching: %+v\n", ld))
	}
}
