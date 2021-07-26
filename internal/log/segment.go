package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/yourfavoritedev/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// newSegment is called when the current active segment hits its max size.
// Opens the store and index files with the appropriate flags or creates new ones if they do not exist.
// Then uses those respective files, storeFile and indexFile to create a new store and new index.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	// opens store file.
	// os.O_CREATE is passed to create the file it it doesn't exist.
	// When the store file is created, we pass os.O_APPEND to make the operating system append to the file when writing.
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	// create a new store using this storeFile
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	// opens index file.
	// os.O_CREATE is passed to create the file it it doesn't exist.
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	// creates a new index using this indexFile
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	// set the segments next offset.
	// if the index is empty, then the next offset would simply be the baseOffset.
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
		// if the index is not empty, then the offset of the next record should take the offset at the end of the segment
		// which we get by adding 1 to the base offset and relative offset.
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append writes the record to the segment's store.
// Then it writes a new entry to the index.
// It returns the offset of the newly appended record.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	// append record to store, as a result, the position of the record is returned
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	// write a new entry to the store using the record's offset and position
	// index offsets are relative to the segment's baseOffset. (Like the segment's nextOffset, the index offset only increases by 1 for each new appended record)
	// we establish the new index offset by subtracting the segment's nextOffset (which increases on every new append) with the segment's baseOffset
	// in practice, if the segment is new and its baseOffset is 0, then its nextOffset is also 0.
	// During the first Append call, we would take 0 - 0, giving us the index offset of 0. The index will identify this entry at a 0 offset.
	// Now on a new append call, if the segment's baseOffset is 16 and its current nextOffset is 20, that means there's already 4 existing records saved to the store and the last used offset in the index is 3.
	// We get the new index offset by taking 20 - 16, giving us an index offset of 4 for the 5th entry.
	if err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)), // this offset refers to the record's offset in relation to the index entries
		pos, // this position refers to the record's position in the stire
	); err != nil {
		return 0, err
	}
	// the nextOffset of the segment is updated in preparation of the next record
	s.nextOffset++
	return cur, nil
}

// Read accepts an offset and uses it navigate an index
// The index holds the position of the record held in the store
// Returns a pointer to that Record
func (s *segment) Read(off uint64) (*api.Record, error) {
	// calculating the relative index offset to grab the position of the record
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	// use position to get record from store
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	// make the bytes readable and set them to the record struct
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size for store bytes or index bytes
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close will close the segment's index and store
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// Remove closes the segment and removes the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// neartestMultiple is used to stay under the user's disc cpacity.
// returns the nearest and lesser multiple of k in j (9, 4) == 8
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
