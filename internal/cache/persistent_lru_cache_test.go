package cache_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/cacheprot"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/fault"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

// newTestStorage creates a cache.Storage backed by the provided data map with
// the given byte limit, using a type assertion to satisfy cache.Storage.
func newTestStorage(t *testing.T, data blobtesting.DataMap, limitBytes uint64) cache.Storage {
	t.Helper()

	return testutil.EnsureType[cache.Storage](t, blobtesting.NewMapStorageWithLimit(data, nil, nil, limitBytes))
}

func TestPersistentLRUCache(t *testing.T) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	const maxSizeBytes = 1000

	cs := testutil.EnsureType[cache.Storage](t, blobtesting.NewMapStorageWithLimit(blobtesting.DataMap{}, nil, nil, maxSizeBytes))

	pc, err := cache.NewPersistentCache(ctx, "testing", cs, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
	}, nil, clock.Now)
	require.NoError(t, err)

	verifyNotCached(ctx, t, pc, "key") // no hits on an empty cache

	someData := bytes.Repeat([]byte{1}, 300)

	pc.Put(ctx, "key1", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key1")

	pc.Put(ctx, "key2", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key2")
	pc.Put(ctx, "key3", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key3")
	pc.Put(ctx, "key4", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key4")

	verifyCached(ctx, t, pc, "key2", someData)

	// final sweep is performed on close at which time key1 becomes candidate
	// for expulsion from cache because it's the oldest and we have 1200 bytes in the cache
	// but the limit is only 1000.
	pc.Close(ctx)

	verifyBlobDoesNotExist(ctx, t, cs, "key1")
	verifyBlobExists(ctx, t, cs, "key2")
	verifyBlobExists(ctx, t, cs, "key3")
	verifyBlobExists(ctx, t, cs, "key4")

	pc, err = cache.NewPersistentCache(ctx, "testing", cs, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
	}, nil, clock.Now)
	require.NoError(t, err)

	verifyNotCached(ctx, t, pc, "key1")
	verifyCached(ctx, t, pc, "key2", someData)
	verifyCached(ctx, t, pc, "key3", someData)
	verifyCached(ctx, t, pc, "key4", someData)

	// create another persistent cache based on the same storage but wrong protection key.
	// all reads from cache will be invalid, which means GetOrLoad will fetch them from the source.
	pc2, err := cache.NewPersistentCache(ctx, "testing", cs, cacheprot.ChecksumProtection([]byte{3, 2, 1}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
	}, nil, clock.Now)
	require.NoError(t, err)

	someError := errors.New("some error")

	var tmp2 gather.WriteBuffer
	defer tmp2.Close()

	require.NoError(t, pc2.GetOrLoad(ctx, "key2", func(output *gather.WriteBuffer) error {
		output.Append([]byte{1, 2, 3})
		return nil
	}, &tmp2))

	require.NoError(t, pc2.GetOrLoad(ctx, "key2", func(output *gather.WriteBuffer) error {
		return someError
	}, &tmp2))

	// make sure we received data returned by the callback.
	require.Equal(t, []byte{1, 2, 3}, tmp2.ToByteSlice())

	// at this point 'cs' was updated with a different checksum, so attempting to read it using
	// 'pc' will return cache miss.
	verifyNotCached(ctx, t, pc, "key2")

	require.ErrorIs(t, pc2.GetOrLoad(ctx, "key9", func(output *gather.WriteBuffer) error {
		return someError
	}, &tmp2), someError)
}

type faultyCache struct {
	*blobtesting.FaultyStorage
}

func (faultyCache) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) (time.Time, error) {
	return time.Time{}, nil
}

func TestPersistentLRUCache_Invalid(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	someError := errors.New("some error")

	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	fs.AddFault(blobtesting.MethodGetMetadata).ErrorInstead(someError)

	pc, err := cache.NewPersistentCache(ctx, "test", fc, nil, cache.SweepSettings{}, nil, clock.Now)
	require.ErrorIs(t, err, someError)
	require.Nil(t, pc)
}

func TestPersistentLRUCache_GetDeletesInvalidBlob(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	someError := errors.New("some error")

	data := blobtesting.DataMap{}

	const maxSizeBytes = 1000

	st := blobtesting.NewMapStorageWithLimit(data, nil, nil, maxSizeBytes)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{MaxSizeBytes: maxSizeBytes}, nil, clock.Now)
	require.NoError(t, err)

	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	verifyCached(ctx, t, pc, "key", []byte{1, 2, 3})

	// corrupt cached data
	data["key"][0] ^= 1

	// simulate failure when trying to delete.
	fs.AddFault(blobtesting.MethodDeleteBlob).ErrorInstead(someError)

	// retrieving should not return anything after the data is corrupted
	verifyNotCached(ctx, t, pc, "key")
}

func TestPersistentLRUCache_PutIgnoresStorageFailure(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	someError := errors.New("some error")

	data := blobtesting.DataMap{}

	st := blobtesting.NewMapStorage(data, nil, nil)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{}, nil, clock.Now)
	require.NoError(t, err)

	fs.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someError)

	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	verifyNotCached(ctx, t, pc, "key")

	require.Equal(t, 1, fs.NumCalls(blobtesting.MethodPutBlob))
}

func TestPersistentLRUCache_SweepMinSweepAge(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	data := blobtesting.DataMap{}

	const maxSizeBytes = 1000

	st := blobtesting.NewMapStorageWithLimit(data, nil, nil, maxSizeBytes)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
		MinSweepAge:  10 * time.Second,
	}, nil, clock.Now)
	require.NoError(t, err)
	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	pc.Put(ctx, "key2", gather.FromSlice(bytes.Repeat([]byte{1, 2, 3}, 10)))
	time.Sleep(1 * time.Second)

	// simulate error during final sweep
	fs.AddFault(blobtesting.MethodListBlobs).ErrorInstead(errors.New("some error"))
	pc.Close(ctx)

	// both keys are retained since we're under min sweep age
	require.Len(t, data, 2)
}

func TestPersistentLRUCache_SweepIgnoresErrors(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	data := blobtesting.DataMap{}

	const maxSizeBytes = 1000

	st := blobtesting.NewMapStorageWithLimit(data, nil, nil, maxSizeBytes)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
	}, nil, clock.Now)
	require.NoError(t, err)

	// ignore delete errors forever
	fs.AddFault(blobtesting.MethodDeleteBlob).ErrorInstead(errors.New("some delete error")).Repeat(1e6)

	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	pc.Put(ctx, "key2", gather.FromSlice(bytes.Repeat([]byte{1, 2, 3}, 10)))
	time.Sleep(500 * time.Millisecond)

	// simulate error during sweep
	fs.AddFaults(blobtesting.MethodListBlobs, fault.New().ErrorInstead(errors.New("some error")))

	time.Sleep(500 * time.Millisecond)

	pc.Close(ctx)

	// both keys are retained since we're under min sweep age
	require.Len(t, data, 2)
}

func TestPersistentLRUCache_Sweep1(t *testing.T) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	data := blobtesting.DataMap{}

	const maxSizeBytes = 1

	st := blobtesting.NewMapStorageWithLimit(data, nil, nil, maxSizeBytes)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
		MinSweepAge:  0 * time.Second,
	}, nil, clock.Now)
	require.NoError(t, err)
	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	pc.Put(ctx, "key", gather.FromSlice(bytes.Repeat([]byte{1, 2, 3}, 1e6)))
	time.Sleep(1 * time.Second)

	// simulate error during final sweep
	fs.AddFaults(blobtesting.MethodListBlobs, fault.New().ErrorInstead(errors.New("some error")))
	pc.Close(ctx)
}

func TestPersistentLRUCacheNil(t *testing.T) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	var pc *cache.PersistentCache

	// no-op
	pc.Close(ctx)
	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	verifyNotCached(ctx, t, pc, "key")

	var tmp gather.WriteBuffer
	defer tmp.Close()

	called := false

	dummyError := errors.New("dummy error")

	require.ErrorIs(t, pc.GetOrLoad(ctx, "key", func(output *gather.WriteBuffer) error {
		called = true
		return dummyError
	}, &tmp), dummyError)

	require.True(t, called)
}

func TestPersistentLRUCache_Defaults(t *testing.T) {
	cacheDir := testutil.TempDirectory(t)
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	const maxSizeBytes = 1000

	cs, err := cache.NewStorageOrNil(ctx, cacheDir, maxSizeBytes, "subdir")
	require.NoError(t, err)

	pc, err := cache.NewPersistentCache(ctx, "testing", cs, nil, cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
	}, nil, clock.Now)
	require.NoError(t, err)

	defer pc.Close(ctx)

	pc.Put(ctx, "key1", gather.FromSlice([]byte{1, 2, 3}))
	verifyCached(ctx, t, pc, "key1", []byte{1, 2, 3})
}

func verifyCached(ctx context.Context, t *testing.T, pc *cache.PersistentCache, key string, want []byte) {
	t.Helper()

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if want == nil {
		require.False(t, pc.TestingGetFull(ctx, key, &tmp))
	} else {
		require.True(t, pc.TestingGetFull(ctx, key, &tmp))
		require.Equalf(t, want, tmp.ToByteSlice(), "invalid cached result for '%s'", key)
	}
}

func verifyNotCached(ctx context.Context, t *testing.T, pc *cache.PersistentCache, key string) {
	t.Helper()

	verifyCached(ctx, t, pc, key, nil)
}

func verifyBlobExists(ctx context.Context, t *testing.T, st cache.Storage, blobID blob.ID) {
	t.Helper()

	_, err := st.GetMetadata(ctx, blobID)
	require.NoErrorf(t, err, "blob '%s'", blobID)
}

func verifyBlobDoesNotExist(ctx context.Context, t *testing.T, st cache.Storage, blobID blob.ID) {
	t.Helper()

	_, err := st.GetMetadata(ctx, blobID)
	require.ErrorIsf(t, err, blob.ErrBlobNotFound, "expected blob not found for '%s'", blobID)
}

// TestPersistentLRUCache_DiskFreeSpaceAmple verifies that when free disk space
// is plentiful, MaxSizeBytes is used unchanged as the effective soft limit.
func TestPersistentLRUCache_DiskFreeSpaceAmple(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	const maxSizeBytes = 1000

	data := blobtesting.DataMap{}
	st := newTestStorage(t, data, maxSizeBytes)

	// Report very large free space – MaxSizeBytes should govern.
	const ampleFreeDiskBytes uint64 = 100 * 1024 * 1024 * 1024 // 100 GiB

	pc, err := cache.NewPersistentCache(ctx, "testing", st, nil, cache.SweepSettings{
		MaxSizeBytes:    maxSizeBytes,
		DiskFreeSpaceFn: func() (uint64, error) { return ampleFreeDiskBytes, nil },
	}, nil, clock.Now)
	require.NoError(t, err)

	// Put 4 items of 300 bytes each → total 1200 bytes, above the 1000-byte limit.
	// Oldest item must be swept.
	item := bytes.Repeat([]byte{1}, 300)

	pc.Put(ctx, "k1", gather.FromSlice(item))
	pc.Put(ctx, "k2", gather.FromSlice(item))
	pc.Put(ctx, "k3", gather.FromSlice(item))
	pc.Put(ctx, "k4", gather.FromSlice(item))

	pc.Close(ctx)

	// k1 should have been evicted; k2-k4 retained (same as default sweep behavior).
	require.NotContains(t, data, blob.ID("k1"), "oldest item must be evicted when above MaxSizeBytes")
	require.Contains(t, data, blob.ID("k2"), "k2 must be retained")
	require.Contains(t, data, blob.ID("k3"), "k3 must be retained")
	require.Contains(t, data, blob.ID("k4"), "k4 must be retained")
}

// TestPersistentLRUCache_DiskFreeSpaceLow verifies that when free disk space
// drops below MinFreeDiskBytes the effective limit is reduced and items are
// swept even when the cache is below MaxSizeBytes.
func TestPersistentLRUCache_DiskFreeSpaceLow(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	// Use a large MaxSizeBytes so the configured limit would never trigger a
	// sweep on its own; only the disk-space check should trigger it.
	const maxSizeBytes = 10 * 1024 * 1024 // 10 MiB

	data := blobtesting.DataMap{}
	st := newTestStorage(t, data, maxSizeBytes)

	// Start with ample free space so initial items are admitted.
	freeBytes := uint64(cache.MinFreeDiskBytes) * 10

	pc, err := cache.NewPersistentCache(ctx, "testing", st, nil, cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
		MinSweepAge:  0, // allow immediate eviction
		DiskFreeSpaceFn: func() (uint64, error) {
			return freeBytes, nil
		},
	}, nil, clock.Now)
	require.NoError(t, err)

	// Write two items while space is ample.
	item := bytes.Repeat([]byte{2}, 300)

	pc.Put(ctx, "k1", gather.FromSlice(item))
	pc.Put(ctx, "k2", gather.FromSlice(item))

	require.Len(t, data, 2, "both items should be cached with ample free space")

	// Simulate free space dropping below MinFreeDiskBytes.
	// Set it to 0 so the effective limit drops below the current cache size.
	freeBytes = 0

	// Adding a third item triggers sweepLocked which now sees 0 free bytes;
	// the effective limit = cacheOnDisk + 0 - MinFreeDiskBytes < 0, so the
	// sweep must evict the oldest item(s).
	pc.Put(ctx, "k3", gather.FromSlice(item))

	pc.Close(ctx)

	// At least one item must have been evicted to bring the cache under the
	// disk-space-adjusted effective limit.
	require.Less(t, len(data), 3, "items must be evicted when free disk space is below MinFreeDiskBytes")
}

// TestPersistentLRUCache_DiskFreeSpaceErrorFallback verifies that when
// DiskFreeSpaceFn returns an error the cache falls back to MaxSizeBytes,
// preserving the existing behavior.
func TestPersistentLRUCache_DiskFreeSpaceErrorFallback(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	const maxSizeBytes = 1000

	data := blobtesting.DataMap{}
	st := newTestStorage(t, data, maxSizeBytes)

	diskErr := errors.New("disk query failed")

	pc, err := cache.NewPersistentCache(ctx, "testing", st, nil, cache.SweepSettings{
		MaxSizeBytes:    maxSizeBytes,
		DiskFreeSpaceFn: func() (uint64, error) { return 0, diskErr },
	}, nil, clock.Now)
	require.NoError(t, err)

	// Same scenario as TestPersistentLRUCache: 4 × 300 bytes > 1000-byte limit.
	item := bytes.Repeat([]byte{3}, 300)

	pc.Put(ctx, "k1", gather.FromSlice(item))
	pc.Put(ctx, "k2", gather.FromSlice(item))
	pc.Put(ctx, "k3", gather.FromSlice(item))
	pc.Put(ctx, "k4", gather.FromSlice(item))

	pc.Close(ctx)

	// With a failing DiskFreeSpaceFn, behavior matches the default sweep:
	// MaxSizeBytes governs and k1 is evicted.
	require.NotContains(t, data, blob.ID("k1"), "k1 must be evicted by MaxSizeBytes fallback")
	require.Contains(t, data, blob.ID("k2"))
	require.Contains(t, data, blob.ID("k3"))
	require.Contains(t, data, blob.ID("k4"))
}
