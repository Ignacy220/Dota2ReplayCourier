package downloader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// MaxReplayCompressedBytes caps in-memory compressed .dem.bz2 downloads at 150 MB to prevent memory exhaustion.
	MaxReplayCompressedBytes = 150 * 1024 * 1024
)

var (
	ErrReplayNotFound404 = errors.New("replay returned HTTP 404 from CDN")
	ErrQuotaExceeded     = errors.New("daily bandwidth quota exceeded")
)

// ProgressReport contains real-time download progress metrics.
type ProgressReport struct {
	BytesDownloaded int64   `json:"bytes_downloaded"`
	TotalBytes      int64   `json:"total_bytes"`
	Percent         float64 `json:"percent"`
}

// Result holds the completed in-RAM compressed replay buffer.
type Result struct {
	CompressedBz2 []byte
	SizeBytes     int64
	Duration      time.Duration
}

// Downloader downloads full .dem.bz2 files into RAM without touching the disk.
type Downloader struct {
	httpClient *http.Client
}

// NewDownloader creates a new in-memory replay Downloader.
func NewDownloader() *Downloader {
	return &Downloader{
		httpClient: &http.Client{
			Timeout: 10 * time.Minute,
		},
	}
}

// DownloadToRAM fetches the entire .dem.bz2 file from url into a RAM []byte slice.
// Note: Even if a game launches mid-download, ctx is only canceled if the user forcefully quits the app,
// allowing active downloads to drain to completion in RAM before pausing.
func (d *Downloader) DownloadToRAM(
	ctx context.Context,
	url string,
	speedLimitKBps int,
	onProgress func(ProgressReport),
	onBytesRead func(uint64),
) (*Result, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "DotaParser-Client/1.2.0")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrReplayNotFound404
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d from %s", resp.StatusCode, url)
	}

	totalBytes := resp.ContentLength
	initialCap := 1024 * 1024
	if totalBytes > 0 && totalBytes <= MaxReplayCompressedBytes {
		initialCap = int(totalBytes)
	}

	buf := bytes.NewBuffer(make([]byte, 0, initialCap))
	limitedReader := io.LimitReader(resp.Body, MaxReplayCompressedBytes)

	chunk := make([]byte, 32*1024)
	var downloaded int64
	windowStart := time.Now()
	var windowBytes int64

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		n, readErr := limitedReader.Read(chunk)
		if n > 0 {
			_, _ = buf.Write(chunk[:n])
			downloaded += int64(n)
			windowBytes += int64(n)

			if onBytesRead != nil {
				onBytesRead(uint64(n))
			}

			if onProgress != nil {
				pct := 0.0
				if totalBytes > 0 {
					pct = (float64(downloaded) / float64(totalBytes)) * 100.0
					if pct > 100.0 {
						pct = 100.0
					}
				} else {
					// Estimate against a typical 45 MB replay if Content-Length was omitted
					pct = (float64(downloaded) / float64(45*1024*1024)) * 100.0
					if pct > 99.0 {
						pct = 99.0
					}
				}
				onProgress(ProgressReport{
					BytesDownloaded: downloaded,
					TotalBytes:      totalBytes,
					Percent:         pct,
				})
			}

			// Optional bandwidth speed limiter
			if speedLimitKBps > 0 {
				maxBytesPerSec := int64(speedLimitKBps) * 1024
				elapsed := time.Since(windowStart)
				if elapsed > 0 {
					expectedDuration := time.Duration(float64(windowBytes) / float64(maxBytesPerSec) * float64(time.Second))
					if expectedDuration > elapsed {
						time.Sleep(expectedDuration - elapsed)
					}
				}
				if time.Since(windowStart) >= time.Second {
					windowStart = time.Now()
					windowBytes = 0
				}
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, readErr
		}
	}

	if onProgress != nil {
		onProgress(ProgressReport{
			BytesDownloaded: downloaded,
			TotalBytes:      downloaded,
			Percent:         100.0,
		})
	}

	return &Result{
		CompressedBz2: buf.Bytes(),
		SizeBytes:     downloaded,
		Duration:      time.Since(start),
	}, nil
}
