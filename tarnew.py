#!/usr/bin/env python2.6
import os
import threading
import sys
from subprocess import Popen, PIPE
from tarfile import TarFile
from tempfile import mkdtemp
from time  import sleep
from collections import defaultdict
from guppy import hpy
import gc

class _MemberDummy(list):
    def append(self, crap):
        pass
    
class MemFriendlyTarFile(TarFile):
    def __init__(self, *args, **kwargs):
        super(MemFriendlyTarFile, self).__init__(*args, **kwargs)
        self.members = _MemberDummy()
    for f in ("getmembers", "getmember", "getnames","_getmember"):
        exec "def {0}(self,*args,**kwargs): print 'UNIMPLEMENTED'".format(f)

class FSCopyError(Exception): pass
class FSCopy(object):

    """
    Provides common exception handling for fs copy objects, cleanup is supposed
    to run when the copy function terminates or has an exception raised
    """
    exception_list = (Exception,)
    _persistent_vars = defaultdict(lambda: None)
    
    def __call__(self, inpath, outpath, **kwargs):
        try:
            self.fscopy(inpath, outpath, **kwargs)
        finally:
            self.cleanup()
    __init__ = __call__

class MountError(Exception): pass


class TarArchive(FSCopy):
    def fscopy(self, path, outfile, **noargs):
        """
        Path is a filesystem which is mountable, outfile is a file object
        """
                        
        t = MemFriendlyTarFile.open(fileobj = outfile, mode = "w|")
        mountpoint = mkdtemp()
        
        self._persistent_vars['mountpoint'] = mountpoint
        
        p = Popen(
            ["/bin/mount", path, mountpoint, '-o', 'ro'], stderr = PIPE)
        stdout, stderr = p.communicate()
        print "MOUNTED"
        if p.returncode != 0:
            raise MountError(stderr)
            
        self._persistent_vars['tar_obj'] = t
        self._persistent_vars['outfile'] = outfile
        
        os.chdir(mountpoint)
        
        
        #os.path.walk(".", _taradd, t)
        #for dirpath, dirnames, filenames in os.walk("."):
        #    for d in dirnames:
        #        d_name = os.path.join(dirpath, d)
        #        t_info = t.gettarinfo(name=d_name)
        #        t.addfile(t_info)
        #        del t_info
        #    for f in filenames:
        #        f_obj = None
        #        t_info = None
        #        f_name = None
        #
        #        try:
        #            f_name = os.path.join(dirpath, f)
        #            f_obj = open(f_name, "rb")
        #            self._persistent_vars['f'] = f_obj
        #            t_info = t.gettarinfo(fileobj=f_obj)
        #            t.addfile(tarinfo=t_info, fileobj=f_obj)
        #        finally:
        #            if f_obj:
        #                f_obj.close()
        #                del f_obj
        #                del f_name
        #                del t_info
        t.add(".")
        self._persistent_vars['progress'] = False
    
    def cleanup(self):
        #Stop the progress counter, it will quite on the next iteration
        self._persistent_vars['progress'] = False
        
        os.chdir('/tmp')
        if self._persistent_vars['tar_obj']:
            try:
                self._persistent_vars['tar_obj'].close()
            except IOError as err:
                print "Warning! Tar archive will be corrupted! :", err
                try :
                    self._persistent_vars['f'].close()
                except (IOError, OSError, AttributeError, ValueError) as err:
                    print err
        
        if self._persistent_vars['outfile']:
            self._persistent_vars['outfile'].close()
        
        if not self._persistent_vars['mountpoint']:
            return
        
        p = Popen(['mountpoint', '-q', self._persistent_vars['mountpoint']])
        p.communicate()
        rt = p.returncode
        
        if rt == 0:
            p = Popen(['umount', self._persistent_vars['mountpoint']],
                                     stderr = PIPE)
            um_stdout, um_stderr = p.communicate()
            if p.returncode != 0:
                lsof = Popen(['lsof', '-n'], stdout=PIPE)
                lsof_stdout, lsof_stderr = lsof.communicate()
                for line in lsof_stdout.split("\n"):
                    if self._persistent_vars['mountpoint'] in line:
                        print line
                raise OSError(um_stderr)
    
        
        if self._persistent_vars['mountpoint']:
            os.rmdir(self._persistent_vars['mountpoint'])
    
    suffix = "tar"

if __name__ == "__main__":
    f = open("/stuff/foo.tar", "wb")
    TarArchive("/dev/xen-vg/CentOS", f)
    f.close()
