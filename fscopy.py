#!/usr/bin/python2.6

import os
import subprocess
import re
import ufs
import threading
import sys

from tempfile import mkdtemp
from subprocess import Popen, PIPE
from time import sleep
from _fscopybase import FSCopy
from _tarsupport import SaneTarFile

class MountError(Exception): pass


class TarArchive(FSCopy):
    def fscopy(self, path, outfile, **noargs):
        """
        Path is a filesystem which is mountable, outfile is a file object
        """
        def _progress(tar_obj, parent_thread):
            #Get used space:
            stat_result = os.statvfs(self._persistent_vars['mountpoint'])
            used_size_mb = ((stat_result.f_blocks - stat_result.f_bfree) * stat_result.f_bsize) // (1024**2)
            
            while parent_thread.isAlive() and self._persistent_vars['progress']:
                sleep(0.3)
                sys.stdout.write("{0:60}\r".format(""))
                sys.stdout.write("{0}/{1} MB".format(
                    tar_obj.bytes_read // 1024**2 ,used_size_mb))
                sys.stdout.flush()
            sys.stdout.write("\n")
            sys.stdout.flush()
            
        t = SaneTarFile.open(fileobj = outfile, mode = "w|")
        mountpoint = mkdtemp()
        
        self._persistent_vars['mountpoint'] = mountpoint
        
        p = Popen(
            ["/bin/mount", path, mountpoint, '-o', 'ro'], stderr = subprocess.PIPE)
        stdout, stderr = p.communicate()
        print "MOUNTED"
        if p.returncode != 0:
            raise MountError(stderr)
            
        self._persistent_vars['tar_obj'] = t
        self._persistent_vars['outfile'] = outfile
        
        os.chdir(mountpoint)
        
        progress_th = None
        if os.isatty(sys.stdout.fileno()):
            self._persistent_vars['progress'] = True
            
        parent_thread = threading.currentThread()
        progress_th = threading.Thread(target=_progress,
                                       name="progress",
                                       args=(t, parent_thread)
                                      )
        progress_th.start()
        
        regex = re.compile("^/dev/|^/proc/|^/tmp/")
        t.add(".",recursive=True, exclude=lambda fname: bool(regex.match(fname)))
    
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
                    os.close(self._persistent_vars['tar_obj'].f_obj.fileno())
                except (IOError, OSError, AttributeError, ValueError) as err:
                    print err
        
        if self._persistent_vars['outfile']:
            self._persistent_vars['outfile'].close()
        
        p = Popen(['mountpoint', '-q', self._persistent_vars['mountpoint']])
        p.communicate()
        rt = p.returncode
        
        if rt == 0:
            p = Popen(['umount', self._persistent_vars['mountpoint']],
                                     stderr = subprocess.PIPE)
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
    
class ImageDump(FSCopy):
    def fscopy(self, path, outfile, **noargs):
        buf = True
        path = open(path, "rb")
        #Get size:
        total = path.seek(0, os.SEEK_END) // 1024**2
        
        def _progress(parent_thread):
            while parent_thread.isAlive() and self._persistent_vars['progress']:
                sleep(0.3)
                sys.stdout.write("{0:60}\r".format(""))
                sys.stdout.write("{0}/{1} MB".format(
                    path.tell() // 1024**2 ,total))
                sys.stdout.flush()
            sys.stdout.write("\n")
            sys.stdout.flush()
        
        #Get ourselves:
        parent_th = threading.currentThread
        
        #Create progress thread:
        progress_th = threading.Thread(target=_progress, name="progress_th", args=(parent_th,))
        self._persistent_vars['progress'] = True
        progress_th.start()
        
        while buf:
            buf = path.read(4*(1024**2))
            outfile.write(buf)
            
        path.close()
        self._persistent_vars['progress'] = False
    
    suffix = "img"
    cleanup = lambda x: None
    

class NTFSClone(FSCopy):
    def fscopy(self, path, outfile, **noargs):
        p = Popen(
            ["ntfsclone", "-o", '-', path],
            stdout = outfile)
        stdout, stderr = p.communicate()
    
    suffix = "ntfs"
    cleanup = lambda x: None
    
class UFSDump(FSCopy):
    suffix = "ufs"
    exception_list = (ufs.UFSError,)
    
    def fscopy(self, path, outfile, **noargs):
        infile = open(path, "rb")
        
        fragsize, blklist = ufs.get_free_blocks(path)
        ufs.copy_fs(blklist, fragsize, infile, outfile)
        
        infile.close()
    
    cleanup = lambda x: None
    

class SwapInfo(FSCopy):
    suffix = "SWAP_INFO"
    cleanup = lambda x: None
    
    def fscopy(self, infile, outfile, **noargs):
        f = open(infile)
        f.seek(0, os.SEEK_END)
        end = f.tell()
        f.close()
        outfile.write(str(end))
        outfile.close()
