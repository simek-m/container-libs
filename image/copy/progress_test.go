package copy

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vbauerster/mpb/v8/decor"
	"go.podman.io/image/v5/types"
)

// TestNewProgressReporter verifies that constructing a reporter
// signals a new artifact event.
func TestNewProgressReporter(t *testing.T) {
	channel := make(chan types.ProgressProperties, 1)
	artifact := types.BlobInfo{}

	r := newProgressReporter(channel, time.Second, artifact)
	assert.NotNil(t, r)
	assert.Equal(t, types.ProgressProperties{
		Event:    types.ProgressEventNewArtifact,
		Artifact: artifact,
	}, <-channel, "constructor should send a new artifact event")
}

// TestProgressReporterReportRead verifies that a read event is sent
// after the interval elapses and not before.
func TestProgressReporterReportRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		channel := make(chan types.ProgressProperties, 1)
		artifact := types.BlobInfo{}
		interval := 5 * time.Second

		r := newProgressReporter(channel, interval, artifact)
		<-channel

		// Before the interval: offset is accumulated, but no event was sent.
		r.reportRead(5)
		assert.Equal(t, uint64(5), r.offset, "offset should be accumulated")
		assert.Equal(t, uint64(5), r.offsetUpdate, "offsetUpdate should be accumulated")

		// Verify that after the interval event was sent.
		time.Sleep(2 * interval)
		go func() {
			r.reportRead(10)
		}()
		res := <-channel
		assert.Equal(t, types.ProgressProperties{
			Event:        types.ProgressEventRead,
			Artifact:     artifact,
			Offset:       15,
			OffsetUpdate: 15,
		}, res, "should send a read event after interval elapses")
	})
}

// TestProgressReporterReportDone verifies that a done event
// includes the accumulated offset.
func TestProgressReporterReportDone(t *testing.T) {
	channel := make(chan types.ProgressProperties, 1)
	artifact := types.BlobInfo{}

	r := newProgressReporter(channel, time.Hour, artifact)
	<-channel

	// Simulate progress.
	r.offset = 50
	r.offsetUpdate = 10

	// Complete.
	go func() {
		r.reportDone()
	}()

	// Verify that the done event was received.
	res := <-channel
	assert.Equal(t, types.ProgressProperties{
		Event:        types.ProgressEventDone,
		Artifact:     artifact,
		Offset:       50,
		OffsetUpdate: 10,
	}, res, "should send a done event with accumulated offsets")
}

// TestProgressReporterReset verifies that reset zeroes the offsets and
// reports a read event.
func TestProgressReporterReset(t *testing.T) {
	channel := make(chan types.ProgressProperties, 1)
	artifact := types.BlobInfo{}

	r := newProgressReporter(channel, time.Hour, artifact)
	<-channel

	// Simulate progress.
	r.offset = 30
	r.offsetUpdate = 15

	// Reset the reporter.
	go func() {
		r.reset()
	}()

	// Verify that a read event was received with zero values.
	res := <-channel
	assert.Equal(t, types.ProgressProperties{
		Event:    types.ProgressEventRead,
		Artifact: artifact,
	}, res, "should send a read event with zeroed offsets")
	assert.Equal(t, uint64(0), r.offset, "offset should be zeroed after reset")
	assert.Equal(t, uint64(0), r.offsetUpdate, "offsetUpdate should be zeroed after reset")
}

func TestCustomPartialBlobDecorFunc(t *testing.T) {
	// A stub test
	s := decor.Statistics{}
	assert.Equal(t, "0.0b / 0.0b (skipped: 0.0b)", customPartialBlobDecorFunc(s))
	// Partial pull in progress
	s = decor.Statistics{}
	s.Current = 1097653
	s.Total = 8329917
	s.Refill = 509722
	assert.Equal(t, "1.0MiB / 7.9MiB (skipped: 497.8KiB = 6.12%)", customPartialBlobDecorFunc(s))
	// Almost complete, but no reuse
	s.Current = int64(float64(s.Total) * 0.95)
	s.Refill = 0
	assert.Equal(t, "7.5MiB / 7.9MiB", customPartialBlobDecorFunc(s))
}
