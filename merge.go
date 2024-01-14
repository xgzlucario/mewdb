package mewdb

// doMerge
func (db *DB) doMerge() (err error) {
	if err = db.dataFiles.Sync(); err != nil {
		return err
	}

	prevSegmentID := db.dataFiles.ActiveSegmentID()
	// create new segment file.
	if err = db.dataFiles.OpenNewActiveSegment(); err != nil {
		return
	}

	record := new(LogRecord)
	var newKeydir Keydir

	err = db.dataFiles.IterWithMax(prevSegmentID, func(keydir Keydir, data []byte) {
		record.decode(data)

		// if key exist in index.
		indexKeydir, ok := db.index.Get(record.Key)
		if ok && keydirEqual(indexKeydir, keydir) {
			// write to new segment file.
			newKeydir, err = db.dataFiles.Write(data)
			if err != nil {
				return
			}
			// update index.
			db.index.Set(record.Key, newKeydir)
		}
	})
	if err != nil {
		return
	}

	// sync data files.
	if err = db.dataFiles.Sync(); err != nil {
		return
	}

	// release lock.
	<-db.mergeC

	// remove old segments.
	return db.dataFiles.RemoveOldSegments(prevSegmentID)
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
