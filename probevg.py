#!/usr/bin/python2.6
"""
THIS FILE IS FOR HISTORICAL PURPOSES ONLY AND REPRESENTS AN EARLY EVOLUTION OF THE
API. DO NOT USE THIS
"""
import subprocess
import sys
import os
import re
import magic
import random
import errno
import partition
import fscopy

from common import BackupGroup, LVMBackupGroup, BackupEntry, FSType

class FSTExists(Exception):  pass


fst_list = set()
bu_group_list = set()

def init_fst_list():
    """
    Initialize list of FSType objects
    """
    
    global fst_list
    #each 2-tuple contains a "human friendly" name of the filesystem type
    #and a regex which matches the output of the 'magic' module when this fs
    #type is found
    
    for nval, obj in enumerate(
        (("Linux", "ext[234]|jfs|reiser|xfs|jfs", fscopy.tarbackup),
        ("UFS_BSD", "unix fast file system", fscopy.ufs_backup),
        ("FreeBSD", None, None),
        ("NTFS", None, fscopy.ntfs_backup),
        ("LVM", "lvm|lvm2",  fscopy.lvm_backup),
        ("SWAP", "swap", None))
    ):
        name, mtxt, bu_method = obj
        fst = FSType(name, nval, mtxt,backupmethod=bu_method)
        fst_list.add(fst)        
        
def scan_vg_entries(bu_vg):
    """
    Probe LVM VG bu_vg for filesystems, disk images and others
    return a list of LVMBackupGroups
    """
    def add_bu_entry(bu_group, path, fst, is_mapping=False):
        assert isinstance(bu_group, BackupGroup)
        tmp = BackupEntry(path, fst, needs_mapping=is_mapping)
        bu_group.add_entry(tmp)
    
    bu_group_list = set()
    
    for entry in os.listdir(os.path.join("/dev", bu_vg)):
        entry = os.path.join("/dev", bu_vg, entry)
        tmp_bu_group = LVMBackupGroup(os.path.basename(entry), bu_vg)
        fst = get_fstype(entry)
        if fst is not None:
            add_bu_entry(tmp_bu_group, entry, fst)
        
        else:
            pt_entries = partition.get_pt_entries(entry)
            pt_mappings = partition.create_mappings(entry, pt_entries)
            
            for subentry in pt_mappings:
                prefix, name = subentry
                fst = get_fstype("/dev/mapper/" + prefix + name)
                if fst is not None:
                    add_bu_entry(tmp_bu_group, name, fst, is_mapping=True)
            
            partition.remove_mappings(pt_mappings)
            
        bu_group_list.add(tmp_bu_group)
        
    return bu_group_list

                
            
def get_fstype(blockdev):
    m = magic.open(magic.MAGIC_RAW)
    m.load()
    buf = open(blockdev, "rb").read(1024**2)
    magictext = m.buffer(buf)
    for fst in fst_list:
        if fst.magicmatch(magictext):
            return fst
    
    return None
    
init_fst_list()