#!/usr/bin/env sh

# Mount an XFS loop filesystem on test-data/, where the integration tests keep their
# files and the unit tests their TMPDIR. The module refuses a disk-dir not on XFS.
# A no-op if test-data/ is already on XFS; otherwise needs sudo and mkfs.xfs (xfsprogs).

set -e

DIR="$(cd "$(dirname "$0")/.." && pwd)/test-data"
IMG="${XFS_IMAGE:-/var/tmp/valkey-large-object-test-data.xfs}"

mkdir -p "$DIR"
if [ "$(stat -f -c %T "$DIR")" != "xfs" ]; then
    # Sparse: the image only uses the space the tests write.
    truncate -s 64G "$IMG"
    mkfs.xfs -q -f "$IMG"
    sudo mount -o loop "$IMG" "$DIR"
    sudo chown "$(id -u):$(id -g)" "$DIR"
fi
mkdir -p "$DIR/tmp"
