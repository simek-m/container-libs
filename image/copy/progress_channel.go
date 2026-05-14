package copy

import (
	"io"
	"time"

	"go.podman.io/image/v5/types"
)

// progressReporter facilitates progress reporting through its
// underlying types.ProgressProperties channel on an interval.
type progressReporter struct {
	channel      chan<- types.ProgressProperties // The reporter channel to which the progress will be sent
	interval     time.Duration                   // The update interval to indicate how often the progress should update
	artifact     types.BlobInfo                  // The blob metadata which is currently being progressed
	lastUpdate   time.Time                       // The last time a progress channel event was sent
	offset       uint64                          // The currently downloaded size in bytes
	offsetUpdate uint64                          // The number of bytes downloaded within the last update interval
}

// reportNewArtifact fires types.ProgressEventNewArtifact to its progress channel.
func (r *progressReporter) reportNewArtifact() {
	r.channel <- types.ProgressProperties{
		Event:    types.ProgressEventNewArtifact,
		Artifact: r.artifact,
	}
	r.lastUpdate = time.Now()
}

// reportRead fires the types.ProgressEventRead event with `bytesRead`
// to its progress channel.
func (r *progressReporter) reportRead(bytesRead uint64) {
	r.offset += bytesRead
	r.offsetUpdate += bytesRead
	if time.Since(r.lastUpdate) > r.interval {
		r.channel <- types.ProgressProperties{
			Event:        types.ProgressEventRead,
			Artifact:     r.artifact,
			Offset:       r.offset,
			OffsetUpdate: r.offsetUpdate,
		}
		r.lastUpdate = time.Now()
		r.offsetUpdate = 0
	}
}

// reportDone fires the ProgressEventDone to its progress channel.
func (r *progressReporter) reportDone() {
	r.channel <- types.ProgressProperties{
		Event:        types.ProgressEventDone,
		Artifact:     r.artifact,
		Offset:       r.offset,
		OffsetUpdate: r.offsetUpdate,
	}
}

// progressReader is an io.Reader that reports its progress to
// an underlying *progressReporter.
type progressReader struct {
	source io.Reader
	*progressReporter
}

// newProgressReader creates a new progress reader for
// `source`:   The source when internally reading bytes
// `channel`:  The reporter channel to which the progress will be sent
// `interval`: The update interval to indicate how often the progress should update
// `artifact`: The blob metadata which is currently being progressed.
func newProgressReader(
	source io.Reader,
	channel chan<- types.ProgressProperties,
	interval time.Duration,
	artifact types.BlobInfo,
) *progressReader {
	r := &progressReader{
		source: source,
		progressReporter: &progressReporter{
			channel:  channel,
			interval: interval,
			artifact: artifact,
		},
	}
	r.reportNewArtifact()
	return r
}

// Read continuously reads bytes into the progress reader and reports the
// status via the internal channel.
func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.source.Read(p)
	r.reportRead(uint64(n))
	return n, err
}
