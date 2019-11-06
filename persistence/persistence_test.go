package persistence_test

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"

	"cloud.google.com/go/datastore"

	"github.com/m-lab/etl-gardener/persistence"
	"github.com/m-lab/go/rtx"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type O1 struct {
	persistence.Base

	Integer int32
}

// Kind implements StateObject.Kind
func (o O1) GetKind() string {
	return reflect.TypeOf(o).Name()
}

func NewO1(name string) O1 {
	return O1{Base: persistence.NewBase(name)}
}

func assertStateObject(so persistence.StateObject) {
	assertStateObject(O1{})
}

func TestDatastoreSaver(t *testing.T) {
	ctx := context.Background()
	ds, err := persistence.NewDatastoreSaver(ctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}

	o := NewO1("foobar")
	o.Integer = 1234
	err = ds.Save(ctx, &o)
	if err != nil {
		t.Fatal(err)
	}

	o.Integer = 0
	err = ds.Fetch(ctx, &o)
	rtx.Must(err, "Fetch error")
	t.Log(o)
	if o.Integer != 1234 {
		t.Error("Integer should be 1234", o)
	}

	err = ds.Delete(ctx, &o)
	rtx.Must(err, "Delete error")
	err = ds.Fetch(ctx, &o)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal("Should have errored")
	}
}

func TestFetchAll(t *testing.T) {
	o := NewO1("foo")
	o.Integer = 1234

	ctx := context.Background()
	ds, err := persistence.NewDatastoreSaver(ctx, "mlab-testing")
	if err != nil {
		t.Fatal(err)
	}

	err = ds.Save(ctx, &o)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		o := NewO1(fmt.Sprint("foo", i))
		o.Integer = (int32)(i)
		err = ds.Save(ctx, &o)
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, objs, err := ds.FetchAll(ctx, O1{})
	if err != nil {
		t.Fatal(err)
	}
	so := objs.([]O1)
	if len(so) != 21 {
		t.Error("Should be 21 items, but got", len(so))
	}

	err = ds.Client.DeleteMulti(ctx, keys)
	rtx.Must(err, "Delete error")
}