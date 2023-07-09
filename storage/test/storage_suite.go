package test

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"

	fixtures "github.com/go-git/go-git-fixtures/v4"
	"github.com/stretchr/testify/suite"
)

type Storer interface {
	storer.EncodedObjectStorer
	storer.ReferenceStorer
	storer.ShallowStorer
	storer.IndexStorer
	config.ConfigStorer
	storage.ModuleStorer
}

type TestObject struct {
	Object plumbing.EncodedObject
	Hash   string
	Type   plumbing.ObjectType
}

type BaseStorage struct {
	Storer Storer

	validTypes  []plumbing.ObjectType
	testObjects map[plumbing.ObjectType]TestObject
}

type BaseStorageSuite struct {
	suite.Suite
	BaseStorage
}

func NewBaseStorage(s Storer) BaseStorage {
	commit := &plumbing.MemoryObject{}
	commit.SetType(plumbing.CommitObject)
	tree := &plumbing.MemoryObject{}
	tree.SetType(plumbing.TreeObject)
	blob := &plumbing.MemoryObject{}
	blob.SetType(plumbing.BlobObject)
	tag := &plumbing.MemoryObject{}
	tag.SetType(plumbing.TagObject)

	return BaseStorage{
		Storer: s,
		validTypes: []plumbing.ObjectType{
			plumbing.CommitObject,
			plumbing.BlobObject,
			plumbing.TagObject,
			plumbing.TreeObject,
		},
		testObjects: map[plumbing.ObjectType]TestObject{
			plumbing.CommitObject: {commit, "dcf5b16e76cce7425d0beaef62d79a7d10fce1f5", plumbing.CommitObject},
			plumbing.TreeObject:   {tree, "4b825dc642cb6eb9a060e54bf8d69288fbee4904", plumbing.TreeObject},
			plumbing.BlobObject:   {blob, "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", plumbing.BlobObject},
			plumbing.TagObject:    {tag, "d994c6bb648123a17e8f70a966857c546b2a6f94", plumbing.TagObject},
		}}
}

func (s *BaseStorageSuite) TearDownTest() {
	fixtures.Clean()
}

func (s *BaseStorageSuite) TestSetEncodedObjectAndEncodedObject() {
	for _, to := range s.testObjects {
		h, err := s.Storer.SetEncodedObject(to.Object)
		s.NoError(err)
		s.Equalf(to.Hash, h.String(), "failed for type %s", to.Type.String())

		o, err := s.Storer.EncodedObject(to.Type, h)
		s.NoError(err)
		s.NoError(objectEquals(o, to.Object))

		o, err = s.Storer.EncodedObject(plumbing.AnyObject, h)
		s.NoError(err)
		s.NoError(objectEquals(o, to.Object))

		for _, t := range s.validTypes {
			if t == to.Type {
				continue
			}

			o, err = s.Storer.EncodedObject(t, h)
			s.Nil(o)
			s.EqualError(err, plumbing.ErrObjectNotFound.Error())
		}
	}
}

func (s *BaseStorageSuite) TestSetEncodedObjectInvalid() {
	o := s.Storer.NewEncodedObject()
	o.SetType(plumbing.REFDeltaObject)

	_, err := s.Storer.SetEncodedObject(o)
	s.Error(err)
}

func (s *BaseStorageSuite) TestIterEncodedObjects() {
	for _, o := range s.testObjects {
		h, err := s.Storer.SetEncodedObject(o.Object)
		s.NoError(err)
		s.Equal(h, o.Object.Hash())
	}

	for _, t := range s.validTypes {
		i, err := s.Storer.IterEncodedObjects(t)
		s.NoErrorf(err, "failed for type %s", t.String())

		o, err := i.Next()
		s.NoError(err)
		s.NoError(objectEquals(o, s.testObjects[t].Object))

		o, err = i.Next()
		s.Nil(o)
		s.EqualErrorf(err, io.EOF.Error(), "failed for type %s", t.String())
	}

	i, err := s.Storer.IterEncodedObjects(plumbing.AnyObject)
	s.NoError(err)

	foundObjects := []plumbing.EncodedObject{}
	i.ForEach(func(o plumbing.EncodedObject) error {
		foundObjects = append(foundObjects, o)
		return nil
	})

	s.Len(foundObjects, len(s.testObjects))
	for _, to := range s.testObjects {
		found := false
		for _, o := range foundObjects {
			if to.Object.Hash() == o.Hash() {
				found = true
				break
			}
		}
		s.Truef(found, "Object of type %s not found", to.Type.String())
	}
}

func (s *BaseStorageSuite) TestPackfileWriter() {
	pwr, ok := s.Storer.(storer.PackfileWriter)
	if !ok {
		s.T().Skip("not a storer.PackWriter")
	}

	pw, err := pwr.PackfileWriter()
	s.NoError(err)

	f := fixtures.Basic().One()
	_, err = io.Copy(pw, f.Packfile())
	s.NoError(err)

	err = pw.Close()
	s.NoError(err)

	iter, err := s.Storer.IterEncodedObjects(plumbing.AnyObject)
	s.NoError(err)
	objects := 0
	err = iter.ForEach(func(plumbing.EncodedObject) error {
		objects++
		return nil
	})
	s.NoError(err)
	s.Equal(objects, 31)
}

func (s *BaseStorageSuite) TestObjectStorerTxSetEncodedObjectAndCommit() {
	storer, ok := s.Storer.(storer.Transactioner)
	if !ok {
		s.T().Skip("not a plumbing.ObjectStorerTx")
	}

	tx := storer.Begin()
	for _, o := range s.testObjects {
		h, err := tx.SetEncodedObject(o.Object)
		s.NoError(err)
		s.Equal(h.String(), o.Hash)
	}

	iter, err := s.Storer.IterEncodedObjects(plumbing.AnyObject)
	s.NoError(err)
	_, err = iter.Next()
	s.EqualError(err, io.EOF.Error())

	err = tx.Commit()
	s.NoError(err)

	iter, err = s.Storer.IterEncodedObjects(plumbing.AnyObject)
	s.NoError(err)

	var count int
	iter.ForEach(func(o plumbing.EncodedObject) error {
		count++
		return nil
	})

	s.Equal(count, 4)
}

func (s *BaseStorageSuite) TestObjectStorerTxSetObjectAndGetObject() {
	storer, ok := s.Storer.(storer.Transactioner)
	if !ok {
		s.T().Skip("not a plumbing.ObjectStorerTx")
	}

	tx := storer.Begin()
	for _, expected := range s.testObjects {
		h, err := tx.SetEncodedObject(expected.Object)
		s.NoError(err)
		s.Equal(h.String(), expected.Hash)

		o, err := tx.EncodedObject(expected.Type, plumbing.NewHash(expected.Hash))
		s.NoError(err)
		s.EqualValues(o.Hash().String(), expected.Hash)
	}
}

func (s *BaseStorageSuite) TestObjectStorerTxGetObjectNotFound() {
	storer, ok := s.Storer.(storer.Transactioner)
	if !ok {
		s.T().Skip("not a plumbing.ObjectStorerTx")
	}

	tx := storer.Begin()
	o, err := tx.EncodedObject(plumbing.AnyObject, plumbing.ZeroHash)
	s.Nil(o)
	s.EqualError(err, plumbing.ErrObjectNotFound.Error())
}

func (s *BaseStorageSuite) TestObjectStorerTxSetObjectAndRollback() {
	storer, ok := s.Storer.(storer.Transactioner)
	if !ok {
		s.T().Skip("not a plumbing.ObjectStorerTx")
	}

	tx := storer.Begin()
	for _, o := range s.testObjects {
		h, err := tx.SetEncodedObject(o.Object)
		s.NoError(err)
		s.Equal(h.String(), o.Hash)
	}

	err := tx.Rollback()
	s.NoError(err)

	iter, err := s.Storer.IterEncodedObjects(plumbing.AnyObject)
	s.NoError(err)
	_, err = iter.Next()
	s.EqualError(err, io.EOF.Error())
}

func (s *BaseStorageSuite) TestSetReferenceAndGetReference() {
	err := s.Storer.SetReference(
		plumbing.NewReferenceFromStrings("foo", "bc9968d75e48de59f0870ffb71f5e160bbbdcf52"),
	)
	s.NoError(err)

	err = s.Storer.SetReference(
		plumbing.NewReferenceFromStrings("bar", "482e0eada5de4039e6f216b45b3c9b683b83bfa"),
	)
	s.NoError(err)

	e, err := s.Storer.Reference(plumbing.ReferenceName("foo"))
	s.NoError(err)
	s.Equal(e.Hash().String(), "bc9968d75e48de59f0870ffb71f5e160bbbdcf52")
}

func (s *BaseStorageSuite) TestCheckAndSetReference() {
	err := s.Storer.SetReference(
		plumbing.NewReferenceFromStrings("foo", "482e0eada5de4039e6f216b45b3c9b683b83bfa"),
	)
	s.NoError(err)

	err = s.Storer.CheckAndSetReference(
		plumbing.NewReferenceFromStrings("foo", "bc9968d75e48de59f0870ffb71f5e160bbbdcf52"),
		plumbing.NewReferenceFromStrings("foo", "482e0eada5de4039e6f216b45b3c9b683b83bfa"),
	)
	s.NoError(err)

	e, err := s.Storer.Reference(plumbing.ReferenceName("foo"))
	s.NoError(err)
	s.Equal(e.Hash().String(), "bc9968d75e48de59f0870ffb71f5e160bbbdcf52")
}

func (s *BaseStorageSuite) TestCheckAndSetReferenceNil() {
	err := s.Storer.SetReference(
		plumbing.NewReferenceFromStrings("foo", "482e0eada5de4039e6f216b45b3c9b683b83bfa"),
	)
	s.NoError(err)

	err = s.Storer.CheckAndSetReference(
		plumbing.NewReferenceFromStrings("foo", "bc9968d75e48de59f0870ffb71f5e160bbbdcf52"),
		nil,
	)
	s.NoError(err)

	e, err := s.Storer.Reference(plumbing.ReferenceName("foo"))
	s.NoError(err)
	s.Equal(e.Hash().String(), "bc9968d75e48de59f0870ffb71f5e160bbbdcf52")
}

func (s *BaseStorageSuite) TestCheckAndSetReferenceError() {
	err := s.Storer.SetReference(
		plumbing.NewReferenceFromStrings("foo", "c3f4688a08fd86f1bf8e055724c84b7a40a09733"),
	)
	s.NoError(err)

	err = s.Storer.CheckAndSetReference(
		plumbing.NewReferenceFromStrings("foo", "bc9968d75e48de59f0870ffb71f5e160bbbdcf52"),
		plumbing.NewReferenceFromStrings("foo", "482e0eada5de4039e6f216b45b3c9b683b83bfa"),
	)
	s.EqualError(err, storage.ErrReferenceHasChanged.Error())

	e, err := s.Storer.Reference(plumbing.ReferenceName("foo"))
	s.NoError(err)
	s.Equal(e.Hash().String(), "c3f4688a08fd86f1bf8e055724c84b7a40a09733")
}

func (s *BaseStorageSuite) TestRemoveReference() {
	err := s.Storer.SetReference(
		plumbing.NewReferenceFromStrings("foo", "bc9968d75e48de59f0870ffb71f5e160bbbdcf52"),
	)
	s.NoError(err)

	err = s.Storer.RemoveReference(plumbing.ReferenceName("foo"))
	s.NoError(err)

	_, err = s.Storer.Reference(plumbing.ReferenceName("foo"))
	s.EqualError(err, plumbing.ErrReferenceNotFound.Error())
}

func (s *BaseStorageSuite) TestRemoveReferenceNonExistent() {
	err := s.Storer.SetReference(
		plumbing.NewReferenceFromStrings("foo", "bc9968d75e48de59f0870ffb71f5e160bbbdcf52"),
	)
	s.NoError(err)

	err = s.Storer.RemoveReference(plumbing.ReferenceName("nonexistent"))
	s.NoError(err)

	e, err := s.Storer.Reference(plumbing.ReferenceName("foo"))
	s.NoError(err)
	s.Equal(e.Hash().String(), "bc9968d75e48de59f0870ffb71f5e160bbbdcf52")
}

func (s *BaseStorageSuite) TestGetReferenceNotFound() {
	r, err := s.Storer.Reference(plumbing.ReferenceName("bar"))
	s.EqualError(err, plumbing.ErrReferenceNotFound.Error())
	s.Nil(r)
}

func (s *BaseStorageSuite) TestIterReferences() {
	err := s.Storer.SetReference(
		plumbing.NewReferenceFromStrings("refs/foo", "bc9968d75e48de59f0870ffb71f5e160bbbdcf52"),
	)
	s.NoError(err)

	i, err := s.Storer.IterReferences()
	s.NoError(err)

	e, err := i.Next()
	s.NoError(err)
	s.Equal(e.Hash().String(), "bc9968d75e48de59f0870ffb71f5e160bbbdcf52")

	e, err = i.Next()
	s.Nil(e)
	s.EqualError(err, io.EOF.Error())
}

func (s *BaseStorageSuite) TestSetShallowAndShallow() {
	expected := []plumbing.Hash{
		plumbing.NewHash("b66c08ba28aa1f81eb06a1127aa3936ff77e5e2c"),
		plumbing.NewHash("c3f4688a08fd86f1bf8e055724c84b7a40a09733"),
		plumbing.NewHash("c78874f116be67ecf54df225a613162b84cc6ebf"),
	}

	err := s.Storer.SetShallow(expected)
	s.NoError(err)

	result, err := s.Storer.Shallow()
	s.NoError(err)
	s.EqualValues(result, expected)
}

func (s *BaseStorageSuite) TestSetConfigAndConfig() {
	expected := config.NewConfig()
	expected.Core.IsBare = true
	expected.Remotes["foo"] = &config.RemoteConfig{
		Name: "foo",
		URLs: []string{"http://foo/bar.git"},
	}

	err := s.Storer.SetConfig(expected)
	s.NoError(err)

	cfg, err := s.Storer.Config()
	s.NoError(err)

	s.EqualValues(cfg.Core.IsBare, expected.Core.IsBare)
	s.EqualValues(cfg.Remotes, expected.Remotes)
}

func (s *BaseStorageSuite) TestIndex() {
	expected := &index.Index{}
	expected.Version = 2

	idx, err := s.Storer.Index()
	s.NoError(err)
	s.EqualValues(idx, expected)
}

func (s *BaseStorageSuite) TestSetIndexAndIndex() {
	expected := &index.Index{}
	expected.Version = 2

	err := s.Storer.SetIndex(expected)
	s.NoError(err)

	idx, err := s.Storer.Index()
	s.NoError(err)
	s.EqualValues(idx, expected)
}

func (s *BaseStorageSuite) TestSetConfigInvalid() {
	cfg := config.NewConfig()
	cfg.Remotes["foo"] = &config.RemoteConfig{}

	err := s.Storer.SetConfig(cfg)
	s.Error(err)
}

func (s *BaseStorageSuite) TestModule() {
	storer, err := s.Storer.Module("foo")
	s.NoError(err)
	s.NotNil(storer)

	storer, err = s.Storer.Module("foo")
	s.NoError(err)
	s.NotNil(storer)
}

func (s *BaseStorageSuite) TestDeltaObjectStorer() {
	dos, ok := s.Storer.(storer.DeltaObjectStorer)
	if !ok {
		s.T().Skip("not an DeltaObjectStorer")
	}

	pwr, ok := s.Storer.(storer.PackfileWriter)
	if !ok {
		s.T().Skip("not a storer.PackWriter")
	}

	pw, err := pwr.PackfileWriter()
	s.NoError(err)

	f := fixtures.Basic().One()
	_, err = io.Copy(pw, f.Packfile())
	s.NoError(err)

	err = pw.Close()
	s.NoError(err)

	h := plumbing.NewHash("32858aad3c383ed1ff0a0f9bdf231d54a00c9e88")
	obj, err := dos.DeltaObject(plumbing.AnyObject, h)
	s.NoError(err)
	s.EqualValues(obj.Type(), plumbing.BlobObject)

	h = plumbing.NewHash("aa9b383c260e1d05fbbf6b30a02914555e20c725")
	obj, err = dos.DeltaObject(plumbing.AnyObject, h)
	s.NoError(err)
	s.EqualValues(obj.Type(), plumbing.OFSDeltaObject)
	_, ok = obj.(plumbing.DeltaObject)
	s.Equal(ok, true)
}

func objectEquals(a plumbing.EncodedObject, b plumbing.EncodedObject) error {
	ha := a.Hash()
	hb := b.Hash()
	if ha != hb {
		return fmt.Errorf("hashes do not match: %s != %s",
			ha.String(), hb.String())
	}

	ra, err := a.Reader()
	if err != nil {
		return fmt.Errorf("can't get reader on a: %q", err)
	}

	rb, err := b.Reader()
	if err != nil {
		return fmt.Errorf("can't get reader on b: %q", err)
	}

	ca, err := io.ReadAll(ra)
	if err != nil {
		return fmt.Errorf("error reading a: %q", err)
	}

	cb, err := io.ReadAll(rb)
	if err != nil {
		return fmt.Errorf("error reading b: %q", err)
	}

	if hex.EncodeToString(ca) != hex.EncodeToString(cb) {
		return errors.New("content does not match")
	}

	err = rb.Close()
	if err != nil {
		return fmt.Errorf("can't close reader on b: %q", err)
	}

	err = ra.Close()
	if err != nil {
		return fmt.Errorf("can't close reader on a: %q", err)
	}

	return nil
}
