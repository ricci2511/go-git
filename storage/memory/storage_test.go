package memory

import (
	"testing"

	"github.com/go-git/go-git/v5/storage/test"
	"github.com/stretchr/testify/suite"
)

type StorageSuite struct {
	test.BaseStorageSuite
}

func (s *StorageSuite) SetupTest() {
	s.BaseStorageSuite.BaseStorage = test.NewBaseStorage(NewStorage())
}

func TestStorageSuite(t *testing.T) {
	suite.Run(t, new(StorageSuite))
}
