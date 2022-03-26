# harmony-leveldb-sharder
# Installation
`go build`

`go install`

# Configuration
## Harmony config
```
[General]
  DataDir = "/data" # change to your database path

[ShardData]
  EnableShardData = true
  DiskCount = 8
  ShardCount = 4
```
## Data_dir Configuration
```
lsblk -l
NAME        MAJ:MIN RM  SIZE RO TYPE  MOUNTPOINT
nvme0n1p1   259:2    0   40G  0 part  /
nvme1n1p1   259:11   0  3.4T  0 part  /mnt/disk00
nvme1n1p2   259:12   0  3.4T  0 part  /mnt/disk01
nvme2n1p1   259:9    0  3.4T  0 part  /mnt/disk02
nvme2n1p2   259:10   0  3.4T  0 part  /mnt/disk03
nvme3n1p1   259:13   0  3.4T  0 part  /mnt/disk04
nvme3n1p2   259:14   0  3.4T  0 part  /mnt/disk05
nvme4n1p1   259:15   0  3.4T  0 part  /mnt/disk06
nvme4n1p2   259:16   0  3.4T  0 part  /mnt/disk07
```
To create directories representing this
`mkdir -p /data/harmony_sharddb_0/<disk[00-07]>`

# Usage
`~/go/bin/kv-compare --from=/path/of/source/dir/harmony_db_0 --to /path/of/dist/dir/harmony_sharddb_0`
