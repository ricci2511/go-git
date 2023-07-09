package transactional

import (
	"testing"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/go-git/go-git/v5/storage/test"
	"github.com/stretchr/testify/suite"
)

type StorageSuite struct {
	test.BaseStorageSuite
	temporal func() storage.Storer
}

func (s *StorageSuite) SetupTest() {
	base := memory.NewStorage()
	temporal := s.temporal()

	s.BaseStorageSuite.BaseStorage = test.NewBaseStorage(NewStorage(base, temporal))
}

func (s *StorageSuite) TestCommit() {
	base := memory.NewStorage()
	temporal := s.temporal()
	st := NewStorage(base, temporal)

	commit := base.NewEncodedObject()
	commit.SetType(plumbing.CommitObject)

	_, err := st.SetEncodedObject(commit)
	s.NoError(err)

	ref := plumbing.NewHashReference("refs/a", commit.Hash())
	s.NoError(st.SetReference(ref))

	err = st.Commit()
	s.NoError(err)

	ref, err = base.Reference(ref.Name())
	s.NoError(err)
	s.EqualValues(ref.Hash(), commit.Hash())

	obj, err := base.EncodedObject(plumbing.AnyObject, commit.Hash())
	s.NoError(err)
	s.EqualValues(obj.Hash(), commit.Hash())
}

func (s *StorageSuite) TestTransactionalPackfileWriter() {
	base := memory.NewStorage()
	temporal := s.temporal()
	st := NewStorage(base, temporal)

	_, tmpOK := temporal.(storer.PackfileWriter)
	_, ok := st.(storer.PackfileWriter)
	s.Equal(ok, tmpOK)
}

func TestStorageSuiteMemoryTemporal(t *testing.T) {
	temporal := func() storage.Storer {
		return memory.NewStorage()
	}

	suite.Run(t, &StorageSuite{temporal: temporal})
}

func TestStorageSuiteFilesystemTemporal(t *testing.T) {
	temporal := func() storage.Storer {
		fs := memfs.New()
		return filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
	}

	suite.Run(t, &StorageSuite{temporal: temporal})
}
