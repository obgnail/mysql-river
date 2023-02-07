package kafka

import (
	"github.com/etcd-io/bbolt"
	"github.com/juju/errors"
)

type store struct {
	db         *bbolt.DB
	bucketName []byte
}

func newStore(path string, bucketName []byte) (*store, error) {
	s := new(store)
	var err error
	s.db, err = bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = s.db.Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			_, err := tx.CreateBucket(bucketName)
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.bucketName = bucketName
	return s, nil
}

func (s *store) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return errors.New("bucket not found")
		}
		value = b.Get(key)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return value, nil
}

func (s *store) Put(key []byte, value []byte) error {
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return errors.New("bucket not found")
		}
		err := b.Put(key, value)
		return errors.Trace(err)
	})
	return errors.Trace(err)
}
