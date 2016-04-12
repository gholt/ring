package ring

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
)

// RingOrBuilder attempts to determine whether a file is a Ring or Builder file
// and then loads it accordingly.
func RingOrBuilder(fileName string) (Ring, *Builder, error) {
	var f *os.File
	var r Ring
	var b *Builder
	var err error
	if f, err = os.Open(fileName); err != nil {
		return r, b, err
	}
	var gf *gzip.Reader
	if gf, err = gzip.NewReader(f); err != nil {
		return r, b, err
	}
	header := make([]byte, 16)
	if _, err = io.ReadFull(gf, header); err != nil {
		return r, b, err
	}
	if string(header[:5]) == "RINGv" {
		if string(header[:16]) != RINGVERSION {
			return r, b, fmt.Errorf("Ring Version missmatch, expected %s found %s", RINGVERSION, header[:16])
		}
		gf.Close()
		if _, err = f.Seek(0, 0); err != nil {
			return r, b, err
		}
		r, err = LoadRing(f)
	} else if string(header[:12]) == "RINGBUILDERv" {
		if string(header[:16]) != BUILDERVERSION {
			return r, b, fmt.Errorf("Builder Version missmatch, expected %s found %s", BUILDERVERSION, header[:16])
		}
		gf.Close()
		if _, err = f.Seek(0, 0); err != nil {
			return r, b, err
		}
		b, err = LoadBuilder(f)
	}
	return r, b, err
}

// PersistRingOrBuilder persists a given ring/builder to the provided filename
func PersistRingOrBuilder(r Ring, b *Builder, filename string) error {
	dir, name := path.Split(filename)
	if dir == "" {
		dir = "."
	}
	_ = os.MkdirAll(dir, 0755)
	f, err := ioutil.TempFile(dir, name+".")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if r != nil {
		err = r.Persist(f)
	} else {
		err = b.Persist(f)
	}
	if err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filename)
}
