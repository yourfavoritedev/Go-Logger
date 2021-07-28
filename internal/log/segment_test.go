package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/yourfavoritedev/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment.test")
	defer os.RemoveAll(dir)

	newRecord := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	// creates a new segment with the baseOffset set to 16
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(newRecord)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		foundRecord, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, newRecord.Value, foundRecord.Value)
	}

	_, err = s.Append(newRecord)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, s.IsMaxed())
	c.Segment.MaxStoreBytes = uint64(len(newRecord.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	// maxed store
	require.True(t, s.IsMaxed())

	// err = s.Remove()
	// require.NoError(t, err)
	// s, err = newSegment(dir, 16, c)
	// require.NoError(t, err)
	// require.False(t, s.IsMaxed())
}
