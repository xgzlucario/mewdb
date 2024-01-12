package mewdb

// doWriteGintFiles generate hint files by current index.
func (db *DB) doWriteHintFiles() (err error) {
	activeSegmentID := db.hintFiles.ActiveSegmentID()

	// create new segment file.
	if err = db.hintFiles.OpenNewActiveSegment(); err != nil {
		return
	}

	// iterate index and encode hint record.
	var record *HintRecord
	db.index.Scan(func(key []byte, ts int64, keydir Keydir) (stop bool) {
		record = &HintRecord{
			Timestamp: uint32(ts / timeCarry),
			Key:       key,
			Keydir:    keydir,
		}
		_, err = db.hintFiles.Write(record.encode())
		return err != nil
	})

	// remove old segments.
	return db.hintFiles.RemoveOldSegments(activeSegmentID)
}
