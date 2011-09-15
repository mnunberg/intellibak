#!/usr/bin/env python2.6

import fscopy
import lvmnested
from fs import FSType

fst_list = []

for nval, obj in enumerate(
    (("Linux", "ext[234]|jfs|reiser|xfs|jfs", fscopy.TarArchive),
    ("UFS_BSD", "unix fast file system", fscopy.UFSDump),
    ("NTFS", None, fscopy.NTFSClone),
    ("LVM", "lvm|lvm2",  lvmnested.LVMNestedBackup),
    ("SWAP", "swap", fscopy.SwapInfo))
):
    name, mtxt, bu_method = obj
    __fst = FSType(name, nval, mtxt,backupmethod=bu_method)
    print __fst.name, "initialized"
    fst_list.append(__fst)

defaultFST = FSType("COPY", -1, "", backupmethod=fscopy.ImageDump)
