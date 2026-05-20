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
	offsetUpdate uint64                          // The number of bytes downloaded since lastUpdate
}

// newProgressReporter creates a new progress reporter
// and immediately reports a new artifact event.
func newProgressReporter(
	channel chan<- types.ProgressProperties,
	interval time.Duration,
	artifact types.BlobInfo,
) *progressReporter {
	channel <- types.ProgressProperties{
		Event:    types.ProgressEventNewArtifact,
		Artifact: artifact,
	}
	return &progressReporter{
		channel:    channel,
		interval:   interval,
		artifact:   artifact,
		lastUpdate: time.Now(),
	}
}

// reset resets the reporters progress
// and reports its zeroed state.
// It's meant to be used on error when
// the processing has to be re-started
// (e.g. ErrFallbackToOrdinaryLayerDownload).
func (r *progressReporter) reset() {
	r.offset = 0
	r.offsetUpdate = 0

	r.channel <- types.ProgressProperties{
		Event:        types.ProgressEventRead,
		Artifact:     r.artifact,
		Offset:       r.offset,
		OffsetUpdate: r.offsetUpdate,
	}
	r.lastUpdate = time.Now()
}

// reportRead reports progress with the number of `bytesRead`.
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

// reportDone reports completion.
func (r *progressReporter) reportDone() {
	r.channel <- types.ProgressProperties{
		Event:        types.ProgressEventDone,
		Artifact:     r.artifact,
		Offset:       r.offset,
		OffsetUpdate: r.offsetUpdate,
	}
}

// progressReader extends a wrapped io.Reader
// with additional reporting of its progress.
type progressReader struct {
	source io.Reader
	*progressReporter
}

// newProgressReader creates a new progress reader that wraps source
// and reports progress through the given reporter.
func newProgressReader(
	source io.Reader,
	reporter *progressReporter,
) *progressReader {
	return &progressReader{
		source:           source,
		progressReporter: reporter,
	}
}

// Read continuously reads bytes into the progress reader and reports the
// status via the internal channel.
func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.source.Read(p)
	r.reportRead(uint64(n))
	return n, err
}
