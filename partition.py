#!/usr/bin/python2.6
"""
Simple interface to get partition table entries from a block device or image;
this also will create mappings for those partition table entries, and remove them
as well.
Like kpartx, but capable of working on images too
"""
LICENSE="""
Python interface for devmapper and partition mappings. Does something similar to
`kpartx`, but more flexible
Copyright (C) 2010  M. Nunberg

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
import re
import os
from collections import namedtuple
import subprocess

class PTEntry(object):
    """
    Class to hold output of sfdisk;
    name is usually in the format of "blkdev[p]$partition_number", and depends
    on the output of sfdisk. The name may be changed so long as it remains the same
    between mapping and unmapping
    """
    def __init__(self, name=None, start=0, end=0, desc=None):
        self.name = name
        self.__start = start
        self.__end = end
        self.__desc = desc
    
    @property
    def start(self): return self.__start
    @property
    def end(self): return self.__end
    @property
    def desc(self): return self.__desc
    
        
        
class PTMappingError(Exception): pass

def get_pt_entries(path):
    """
    gets partition table entries, returns a list of PTEntry objects sorted based
    on their starting position on disk
    """
    p = subprocess.Popen(["/sbin/sfdisk", "-lub", path],
            stdout = subprocess.PIPE, stderr = subprocess.PIPE)    
    stdout, stderr = p.communicate()

    regex = re.compile(
            r".*^\s*Device\s*Boot\s*Start\s*End\s*#sectors\s*Id\s*System\s*$"
            r"(?P<entries>.*)",re.I|re.M|re.S)        
    m = regex.match(stdout)

    entries = None
    if m is not None:
        entries = m.group("entries")
    else:
        return None
    entries = entries.split("\n")
    regex = re.compile(
            r"^(?P<name>\S*)"
            r"\s*\*?\s*"
            r"(?P<start>\d*)\s*"
            r"(-|(?P<end>\d*))\s*"
            r"(?P<sectors>\d*)\s*"
            r"(?P<desc>.*)$",
            re.I|re.S)

    plist = list()
    for entry in entries:
        m = regex.match(entry)
        if m is None or not entry:
            continue
        if not m.group("end") or not m.group("start"):
            continue
        tmp = PTEntry(
                name = m.group("name"),
                start = int(m.group("start")),
                end = int(m.group("end")),
                desc = m.group("desc"))
        plist.append(tmp)

    return sorted(plist, key=lambda x: x.start)

def __dmsetup_create(ptentry, mapping_name, dev):
    tbl_entry = "0 {sectors} linear {dev} {start}".format(
            sectors = ptentry.end - ptentry.start,
            dev = dev,
            start = ptentry.start)
    p = subprocess.Popen(["/sbin/dmsetup", "create", mapping_name],
        stdin = subprocess.PIPE, stderr=subprocess.PIPE)
    
    stdout, stderr = p.communicate(input=tbl_entry)
    if p.returncode != 0:
        raise PTMappingError("dmsetup", mapping_name, stderr)
    return mapping_name

def __dmsetup_remove(mapping_name):
    #GRRRR messed up hack here, but nothing else i can do
    
    attempts = 0
    while attempts < 8:
        ready = False
        while not ready:
            p = subprocess.Popen(["/sbin/dmsetup", "info", mapping_name],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout,stderr = p.communicate()
            for line in stdout.split("\n"):
                if "open count" in line.lower():
                    line = line.split(":")
                    if "0" == line[1].strip():
                        ready = True
                        
        p = subprocess.Popen(["/sbin/dmsetup", "remove", mapping_name],
            stderr = subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            return True
        else:
            print "FAILED.. retrying removal of", mapping_name
            attempts += 1
            
    raise PTMappingError("dmsetup", mapping_name, stderr)
    
def create_mappings(dev, entries, mapping_prefix="BU_SPECIAL_"):
    """
    -> mapping_prefix, entry_name
    """
    import random
    import string
    
    rand_infix = ""
    for i in range(4):
        rand_infix += random.choice(string.ascii_letters)
    rand_infix += "_"
    
    mapping_prefix += rand_infix
    
    maps = []
    for entry in entries:
        mapping_name = mapping_prefix + os.path.basename(entry.name)
        #print "PARTITION:",  mapping_name
        __dmsetup_create(entry, mapping_name, dev)
        maps.append((mapping_prefix, os.path.basename(entry.name)))
    return maps

def remove_mappings(p_mappings):
    for  p_map in p_mappings:
        prefix, name = p_map
        name = prefix + name
        __dmsetup_remove(name)
        
def copy_remaining_dev(pt_entry_list, infile, outfile):
    dev_size = infile.seek(0,  os.SEEK_END)
    byte_map = []
    
    if pt_entry_list[0][0] != 0:
        last_region_end = 0
    else:
        last_region_end = (pt_entry_list[0][1]*512)
        
    outfile_intended_size