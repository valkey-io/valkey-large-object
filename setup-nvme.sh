#!/bin/bash
# NVMe stripe setup for i8ge.48xlarge (or similar multi-NVMe instances).
#
# Idempotent: tears down any existing stripe and recreates from scratch.
# Safe to run repeatedly — produces the same result every time.
#
# Creates an LVM striped volume across all NVMe instance store drives,
# formats with XFS, and mounts at /mnt/bigobj-data.
#
# Usage:
#   sudo ./setup-nvme.sh
#
# Prerequisites:
#   - Instance with NVMe instance store drives (e.g., i8ge.48xlarge)
#   - Root access
#   - Packages: xfsprogs, lvm2

set -e

MOUNT_POINT="/mnt/bigobj-data"
VG_NAME="bigobj_vg"
LV_NAME="bigobj_lv"
LV_PATH="/dev/$VG_NAME/$LV_NAME"

echo "=============================================="
echo "NVMe Stripe Setup"
echo "=============================================="

# ─── Step 1: Discover NVMe instance store drives ─────────────────────────────

echo ""
echo "Discovering NVMe instance store drives..."
DRIVES=()
for dev in /dev/nvme*n1; do
    [ -b "$dev" ] || continue
    # Skip drives with partitions (root volume)
    if lsblk -n "$dev" | grep -q "part"; then
        echo "  Skipping $dev (has partitions — likely root volume)"
        continue
    fi
    # Skip drives < 2TB (EBS volumes, boot disks — instance store is 6.8T+)
    SIZE_BYTES=$(lsblk -dn -b -o SIZE "$dev" 2>/dev/null | tr -d ' ')
    if [ -n "$SIZE_BYTES" ] && [ "$SIZE_BYTES" -lt 2000000000000 ]; then
        echo "  Skipping $dev ($(lsblk -dn -o SIZE "$dev" | tr -d ' ') — too small for instance store)"
        continue
    fi
    SIZE=$(lsblk -dn -o SIZE "$dev" 2>/dev/null | tr -d ' ')
    DRIVES+=("$dev")
    echo "  Found: $dev ($SIZE)"
done

if [ ${#DRIVES[@]} -eq 0 ]; then
    echo "ERROR: No NVMe instance store drives found."
    exit 1
fi

echo ""
echo "Using ${#DRIVES[@]} drives: ${DRIVES[*]}"
echo ""
echo "WARNING: This will DESTROY all data on these drives and $MOUNT_POINT."
read -p "Continue? [y/N] " -r
if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# ─── Step 2: Tear down existing setup (if any) ───────────────────────────────

echo ""
echo "Tearing down existing setup..."

# Unmount
if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    echo "  Unmounting $MOUNT_POINT..."
    umount "$MOUNT_POINT"
fi

# Remove logical volume
if lvs "$VG_NAME/$LV_NAME" &>/dev/null; then
    echo "  Removing logical volume $LV_NAME..."
    lvremove -f "$VG_NAME/$LV_NAME"
fi

# Remove volume group
if vgs "$VG_NAME" &>/dev/null; then
    echo "  Removing volume group $VG_NAME..."
    vgremove -f "$VG_NAME"
fi

# Remove physical volumes
for dev in "${DRIVES[@]}"; do
    if pvs "$dev" &>/dev/null; then
        echo "  Removing physical volume $dev..."
        pvremove -f "$dev"
    fi
done

# Wipe signatures
for dev in "${DRIVES[@]}"; do
    wipefs -a "$dev" &>/dev/null || true
done

echo "  Teardown complete."

# ─── Step 3: Create fresh stripe ─────────────────────────────────────────────

echo ""
echo "Creating fresh stripe..."

# Create physical volumes
echo "  Creating physical volumes..."
for dev in "${DRIVES[@]}"; do
    pvcreate -f "$dev"
done

# Create volume group
echo "  Creating volume group $VG_NAME..."
vgcreate "$VG_NAME" "${DRIVES[@]}"

# Create striped logical volume
STRIPE_COUNT=${#DRIVES[@]}
echo "  Creating striped logical volume ($STRIPE_COUNT-way stripe, 256KB stripe size)..."
lvcreate -l 100%FREE -i "$STRIPE_COUNT" -I 256k -n "$LV_NAME" "$VG_NAME"

# Format with XFS
echo "  Formatting with XFS..."
mkfs.xfs -f "$LV_PATH"

# ─── Step 4: Mount ────────────────────────────────────────────────────────────

echo ""
echo "Mounting at $MOUNT_POINT..."
mkdir -p "$MOUNT_POINT"
mount -o noatime,discard "$LV_PATH" "$MOUNT_POINT"

# Update fstab (remove old entry if exists, add new)
sed -i "\|$VG_NAME/$LV_NAME|d" /etc/fstab
echo "$LV_PATH $MOUNT_POINT xfs noatime,discard 0 0" >> /etc/fstab

# Set permissions for the user who invoked sudo
REAL_USER=$(logname 2>/dev/null || echo ec2-user)
chown "$REAL_USER":"$REAL_USER" "$MOUNT_POINT"

# ─── Done ─────────────────────────────────────────────────────────────────────

echo ""
echo "=============================================="
echo "NVMe stripe ready"
echo "=============================================="
echo ""
df -h "$MOUNT_POINT"
echo ""
echo "Drives:     ${#DRIVES[@]}"
echo "Stripe:     $STRIPE_COUNT-way"
echo "Mount:      $MOUNT_POINT"
echo "Filesystem: XFS"
echo ""
echo "Next: ./bench.sh --port 7399 --disk-dir $MOUNT_POINT/bench-test"
