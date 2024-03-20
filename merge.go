package mewdb

// doMerge
func (db *DB) doMerge() (err error) {
	defer db.mergeLock.Unlock()
	db.log.Info("start merge")

	// get current active segmentId.
	prevSegmentId, err := db.dataFiles.ActiveSegmentID(), db.dataFiles.OpenNewActiveSegment()
	if err != nil {
		return err
	}

	record := new(LogRecord)
	var newKeydir Keydir

	err = db.dataFiles.IterWithMax(prevSegmentId, func(keydir Keydir, data []byte) {
		record.decode(data)

		// if key is the latest version in index, write to new segment file.
		indexKeydir, ok := db.index.Get(record.Key)
		if ok && keydirEqual(indexKeydir, keydir) {
			newKeydir, err = db.dataFiles.Write(data)
			if err != nil {
				return
			}
			db.index.Set(record.Key, newKeydir, record.TTL())
		}
	})
	if err != nil {
		return
	}

	db.log.Info("merge end")

	// remove old segments.
	return db.dataFiles.RemoveOldSegments(prevSegmentId)
}

// keydirEqual
func keydirEqual(a, b Keydir) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}
