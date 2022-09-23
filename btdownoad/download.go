package btdownoad

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/bodgit/sevenzip"
	"golang.org/x/sync/semaphore"
)

const TorrentExt = ".torrent"
const DefaultNetworkChunkSize = 1 * 1024 * 1024
const DefaultTorrentPort = 42069
const DownloadSlot = 10

type client struct {
	cfg      *torrent.ClientConfig
	filePath string
}

func NewClient(path string, port int) (*client, error) {
	if path == "" {
		return nil, errors.New("path can not be empty")
	}

	var err error
	absdatadir, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	path = absdatadir

	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = path
	cfg.Logger = lg.Default.FilterLevel(lg.Info)
	cfg.ListenPort = port
	cfg.Seed = true

	return &client{
		cfg:      cfg,
		filePath: path,
	}, nil
}

func (c *client) TorrentClient() (*torrent.Client, error) {
	tc, err := torrent.NewClient(c.cfg)
	if nil != err {
		return nil, err
	}
	return tc, nil
}

func (c *client) Download(ctx context.Context, t Torrent, log *logrus.Entry) error {
	tc, err := c.TorrentClient()
	if nil != err {
		return err
	}
	defer tc.Close()

	torrentFileName := filepath.Join(c.filePath, t.Filename())
	var metaInfo *metainfo.MetaInfo

	if _, err := os.Stat(torrentFileName); os.IsNotExist(err) {
		// if !common.FileExist(torrentFileName) {
		response, err := http.Get(t.URL())
		if err != nil {
			return fmt.Errorf("error downloading torrent file: %v", err)
		}
		defer response.Body.Close()

		metaInfo, err = metainfo.Load(response.Body)
		if err != nil {
			return fmt.Errorf("error loading torrent file from %q: %v\n", t.URL(), err)
		}
	} else {
		metaInfo, err = metainfo.LoadFromFile(torrentFileName)
		if err != nil {
			return fmt.Errorf("error loading torrent file from %q: %v\n", torrentFileName, err)
		}
	}

	file, err := os.Create(torrentFileName)
	if err != nil {
		return err
	}
	defer file.Close()
	defer file.Sync()

	if err := metaInfo.Write(file); err != nil {
		return err
	}

	tc.AddTorrent(metaInfo)

	var sem = semaphore.NewWeighted(int64(DownloadSlot))
	go func() {
		for {
			torrents := tc.Torrents()
			for _, t := range torrents {
				<-t.GotInfo()
				if t.Complete.Bool() {
					continue
				}
				if err := sem.Acquire(ctx, 1); err != nil {
					return
				}
				t.AllowDataDownload()
				t.DownloadAll()
				go func(t *torrent.Torrent) {
					defer sem.Release(1)
					<-t.Complete.On()
				}(t)
			}
			time.Sleep(30 * time.Second)
		}
	}()

	logEvery := time.NewTicker(5 * time.Second)
	defer logEvery.Stop()

	statInterval := 5 * time.Second
	statEvery := time.NewTicker(statInterval)
	defer statEvery.Stop()
	ds := NewStats()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-statEvery.C:
			ds.ReCalcStats(statInterval, tc)
		case <-logEvery.C:
			stats := ds.Stats()
			if stats.MetadataReady < stats.FilesTotal {
				log.Info("[Download] Waiting for torrents metadata: %d/%d \n", stats.MetadataReady, stats.FilesTotal)
				continue
			}

			if stats.Completed {
				log.Info("[Download] download finished")
				return nil
			}

			log.Info("[Download] Downloading",
				"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, ByteCount(stats.BytesCompleted), ByteCount(stats.BytesTotal)),
				"download", ByteCount(stats.DownloadRate)+"/s",
				"upload", ByteCount(stats.UploadRate)+"/s",
				"peers", stats.PeersUnique,
				"connections", stats.ConnectionsTotal,
				"files", stats.FilesTotal)
			if stats.PeersUnique == 0 {
				ips := tc.BadPeerIPs()
				if len(ips) > 0 {
					log.Info("[Download] Stats", "banned", ips)
				}
			}
		}
	}
}

func Uncompress(ctx context.Context, from, to string) ([]string, error) {
	if !FileExist(from) {
		return nil, errors.New(from + " not exist!")
	}
	if err := os.MkdirAll(to, 0744); nil != err {
		return nil, fmt.Errorf("create dir failed, %v", err)
	}

	r, err := sevenzip.OpenReader(from)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var errStr string
	errCH := make(chan error)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-errCH:
				if nil == err {
					return
				}
				errStr += e.Error()
			}
		}
	}()

	var files []string
	concur := runtime.NumCPU()
	seg := semaphore.NewWeighted(int64(concur))
	for i, f := range r.File {
		if err := seg.Acquire(ctx, 1); nil != err {
			return nil, err
		}
		go func(i int, f *sevenzip.File) {
			defer seg.Release(1)
			rc, err := f.Open()
			if err != nil {
				errCH <- err
				return
			}
			defer rc.Close()

			tmpF, err := os.Create(filepath.Join(to, f.Name))
			if nil != err {
				errCH <- err
				return
			}
			defer tmpF.Close()
			w := bufio.NewWriter(tmpF)
			if _, err := io.Copy(w, rc); err != nil {
				errCH <- err
				return
			}
			w.Flush()
		}(i, f)

		files = append(files, f.Name)
	}
	_ = seg.Acquire(ctx, int64(concur))
	close(errCH)

	if len(errStr) > 0 {
		return nil, errors.New(errStr)
	}
	return files, nil
}

// FileExist checks if a file exists at filePath.
func FileExist(filePath string) bool {
	_, err := os.Stat(filePath)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}
