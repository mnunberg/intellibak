#!/usr/bin/python2.6
import os
import subprocess
import thread

__cls_mappings = dict()

def __register_name(c):
    global __cls_mappings
    __cls_mappings[c.__name__] = c
    return c

def get_compressor(name):
    try:
        return __cls_mappings[name]
    except KeyError: pass
    
    for i in __cls_mappings.items():
        key, value = i
        if key.lower().strip() == name.lower().strip():
            return value
    print "Compressor not found. Using 'Null'"
    return Null

class CompressError(Exception): pass

class CompressObject:
    def __init__(self, level=None, threads=None):
        if level:
            if level < 1:
                print "setting compression level to 1"
                level = 1
            if level > 9:
                print "setting compression level to 9"
                level = 9
        else:
            level = 5

        self.__level = level
        self.__threads =  threads
    @property
    def threads(self):
        return self.__threads
    @property
    def level(self):
        return self.__level
        
    def _base_compress(self, infile, outfile, binargs):
        p = subprocess.Popen(binargs,
                             stdin = infile, stdout = outfile, stderr=subprocess.PIPE,
                             close_fds=True)
        if p.stderr:
            stderr = p.stderr.read()
        p.wait()
        if p.returncode != 0:
            infile.close()
            outfile.close()
            raise CompressError(binargs, stderr)

@__register_name
class XZ(CompressObject):
    suffix = "xz"    
    def compress(self, infile, outfile):
        if self.threads is None:
            threads = os.sysconf("SC_NPROCESSORS_ONLN")
        else:
            threads = self.threads
        
        binargs = ["xz_mt_simple", str(threads), str(self.level)]
        self._base_compress(infile, outfile, binargs)

@__register_name
class Gzip(CompressObject):
    suffix = "gz"   
    def compress(self, infile, outfile):
        binargs = ["gzip", "-" + str(self.level) ]
        self._base_compress(infile, outfile, binargs)

@__register_name
class Bzip(CompressObject):
    suffix = "bz2"
    def compress(self, infile, outfile):
        binargs = ["bzip2", "-" + str(self.level)]
        self._base_compress(infile, outfile, binargs)
        
@__register_name
class Null(CompressObject):
    suffix = "raw"
    def compress(self, infile, outfile):
        binargs = ["cat"]
        self._base_compress(infile, outfile, binargs)
