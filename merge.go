package mewdb

// doMerge
func (db *DB) doMerge() (err error) {
	// get current active segmentId.
	segmentId, err := db.dataFiles.ActiveSegmentID(), db.dataFiles.OpenNewActiveSegment()
	if err != nil {
		return
	}
	record := new(LogRecord)
	var newKeydir Keydir

	err = db.dataFiles.IterWithMax(segmentId, func(keydir Keydir, data []byte) {
		record.decode(data)
		// if key is the latest version in index, write to new segment file.
		indexKeydir, ok := db.index.Get(record.Key)
		if ok && keydirEqual(indexKeydir, keydir) {
			newKeydir, err = db.dataFiles.Write(data)
			if err != nil {
				return
			}
			db.index.Set(record.Key, newKeydir)
		}
	})
	if err != nil {
		return
	}

	// release lock.
	<-db.mergeC

	// remove old segments.
	return db.dataFiles.RemoveOldSegments(segmentId)
}

// keydirEqual
func keydirEqual(a, b Keydir) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset &&
		a.ChunkSize == b.ChunkSize
}

// doWriteGintFiles generate hint files by current index.
// func (db *DB) doWriteHintFiles() (err error) {
// 	activeSegmentID := db.hintFiles.ActiveSegmentID()
// 	// create new segment file.
// 	if err = db.hintFiles.OpenNewActiveSegment(); err != nil {
// 		return
// 	}

// 	// iterate index and encode hint record.
// 	var record *HintRecord
// 	db.index.Scan(func(key []byte, ts int64, keydir Keydir) (stop bool) {
// 		record = &HintRecord{
// 			Timestamp: uint32(ts / timeCarry),
// 			Key:       key,
// 			Keydir:    keydir,
// 		}
// 		_, err = db.hintFiles.Write(record.encode())
// 		return err != nil
// 	})

// 	// remove old segments.
// 	return db.hintFiles.RemoveOldSegments(activeSegmentID)
// }
