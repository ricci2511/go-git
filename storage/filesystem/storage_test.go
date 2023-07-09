package filesystem

import (
	"testing"

	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage/test"
	"github.com/stretchr/testify/suite"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-billy/v5/util"
)

type StorageSuite struct {
	test.BaseStorageSuite
	dir string
	fs  billy.Filesystem
}

func (s *StorageSuite) SetupTest() {
	tmp, err := util.TempDir(osfs.Default, "", "go-git-filestystem-config")
	s.NoError(err)

	s.dir = tmp
	s.fs = osfs.New(s.dir)
	storage := NewStorage(s.fs, cache.NewObjectLRUDefault())

	setUpTest(s, storage)
}

func setUpTest(s *StorageSuite, storage *Storage) {
	// ensure that right interfaces are implemented
	var _ storer.EncodedObjectStorer = storage
	var _ storer.IndexStorer = storage
	var _ storer.ReferenceStorer = storage
	var _ storer.ShallowStorer = storage
	var _ storer.DeltaObjectStorer = storage
	var _ storer.PackfileWriter = storage

	s.BaseStorageSuite.BaseStorage = test.NewBaseStorage(storage)
}

func (s *StorageSuite) TestFilesystem() {
	fs := memfs.New()
	storage := NewStorage(fs, cache.NewObjectLRUDefault())

	s.EqualValues(fs, storage.Filesystem())
}

func (s *StorageSuite) TestNewStorageShouldNotAddAnyContentsToDir() {
	fis, err := s.fs.ReadDir("/")
	s.NoError(err)
	s.Len(fis, 0)
}

func TestStorageSuite(t *testing.T) {
	suite.Run(t, new(StorageSuite))
}

type StorageExclusiveSuite struct {
	StorageSuite
}

func (s *StorageExclusiveSuite) SetupTest() {
	tmp, err := util.TempDir(osfs.Default, "", "go-git-filestystem-config")
	s.NoError(err)

	s.dir = tmp
	s.fs = osfs.New(s.dir)

	storage := NewStorageWithOptions(
		s.fs,
		cache.NewObjectLRUDefault(),
		Options{ExclusiveAccess: true})

	setUpTest(&s.StorageSuite, storage)
}

func TestStorageExclusiveSuite(t *testing.T) {
	suite.Run(t, new(StorageExclusiveSuite))
}
