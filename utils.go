package ring

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"path"
)

// RingOrBuilder attempts to determine whether a file is a Ring or Builder file
// and then loads it accordingly.
func RingOrBuilder(fileName string) (r Ring, b *Builder, err error) {
	var f *os.File
	if f, err = os.Open(fileName); err != nil {
		return
	}
	var gf *gzip.Reader
	if gf, err = gzip.NewReader(f); err != nil {
		return
	}
	header := make([]byte, 16)
	if _, err = io.ReadFull(gf, header); err != nil {
		return
	}
	if string(header[:5]) == "RINGv" {
		gf.Close()
		if _, err = f.Seek(0, 0); err != nil {
			return
		}
		r, err = LoadRing(f)
	} else if string(header[:12]) == "RINGBUILDERv" {
		gf.Close()
		if _, err = f.Seek(0, 0); err != nil {
			return
		}
		b, err = LoadBuilder(f)
	}
	return
}

// PersistRingOrBuilder persists a given ring/builder to the provided filename
func PersistRingOrBuilder(r Ring, b *Builder, filename string) error {
	dir, name := path.Split(filename)
	if dir == "" {
		dir = "."
	}
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
