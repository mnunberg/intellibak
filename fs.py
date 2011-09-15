#!/usr/bin/python2.6
LICENSE="""
This is part of a backup system framework in python
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

import os
import re

class FSType:
    """
    Class to hold basic information about filesystem-specific operations
    and properties
    """
    
    def  __init__(self, name, numval, magictext=None, backupmethod = None):
        self.__name = name
        self.__numval = numval                
        self.__magictext = str(name) if magictext is None else magictext
        if backupmethod is not None:
            assert callable(backupmethod)
        self.__backupmethod = backupmethod
        
    @property
    def name(self): return self.__name
    
    @property
    def backupmethod(self): return self.__backupmethod
    
    def magicmatch(self, text): return re.search(self.__magictext,text, re.I)
    
    def __eq__(self, other): return self.__numval == other.__numval
    def __hash__(self): return self.__numval
    def __str__(self): return str(self.name)

def get_fstype(blockdev, fst_list, seek=0):
    f = open(blockdev, "rb")
    f.seek(seek, os.SEEK_SET)
    buf = f.read(1024**2) #2 MB
    f.close()
    
    #See if we have the native magic module for python
    try:
        import magic
        m = magic.open(magic.MAGIC_RAW)
        m.load()
        magictext = m.buffer(buf)
    
    #if not, use the file binary
    except  ImportError as err:
        print "Can't find 'magic' module. Using 'file' binary"
        import subprocess
        p = subprocess.Popen(['file', '-'],
            stdout = subprocess.PIPE, stdin=subprocess.PIPE)
        stdout, stderr = p.communicate(input=buf)
        magictext = stdout
    #print "DEBUG:", magictext
    #Search the fs type list for a matching type, and return it
    for fst in fst_list:
        if fst.magicmatch(magictext):
            return fst    
    return None
