#!/usr/bin/python2.6
import tarfile
import gc
#gc.set_debug(gc.DEBUG_UNCOLLECTABLE|gc.DEBUG_LEAK)
class _Dummy(object):
    #provide .name
    name = ""
class _listDummy(list):
    def append(self, crap):
        pass
    
class SaneTarFile(tarfile.TarFile):
    """
    I wrote this subclass because the TarFile class has no way of closing whatever
    source file descriptor it has open. This was causing umount to fail because
    tar still had an fd open on the mountpoint. This keeps track of the current
    open fd as an instance variable. There are other issues with it... but meh
    """
    def __init__(self, *args, **kwargs):
        super(SaneTarFile,self).__init__(*args, **kwargs)        
        self.bytes_read = 0
        self.f_obj = _Dummy()
        self.members = _listDummy()
        
    def addfile(self, tarinfo, fileobj = None):
        if fileobj:
            self.f_obj = fileobj
        self.bytes_read += int(tarinfo.size)
        super(SaneTarFile, self).addfile(tarinfo, fileobj)
    
    for f in ("getmembers", "getmember", "getnames","_getmember"):
        exec "def {0}(self,*args,**kwargs): print 'UNIMPLEMENTED'".format(f)
