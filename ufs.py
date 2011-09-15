#!/usr/bin/python2.6
"""This module copies only used blocks from a UFS filesystem, writing unused blocks
as null, in order to make it uniform for compression. Compare to 'ntfsclone'
"""
import os
import sys
import subprocess
import re
import resource
import tempfile
import threading
import time

class UFSError(Exception): pass
class ProgressDummy(object):
    """
    Object to hold variables for progess thread
    """
    cur = 0
    total = 0
    finished = False
        
def _progress_output(dummy_obj, parent_th):
    """
    Simple progress thread
    """
    #TODO: Get terminal size
    while not dummy_obj.finished and parent_th.isAlive():
        #update every 0.1 seconds
        time.sleep(0.1)
        sys.stdout.write("{0:60}\r".format(""))
        sys.stdout.write("{0}/{1} MB\r".format(dummy_obj.cur//1024**2, dummy_obj.total//1024**2))
        sys.stdout.flush()

    print dummy_obj.total, "bytes read!"
    
def get_free_blocks(fs):
    """
    get list of free blocks in a pythonized way
    fs is a block device; -> (fragsize, free_blocks_list);
    free_blocks_list = [(start_free, end_free),..]
    """
    p = subprocess.Popen(["/sbin/dumpfs.ufs", "-m", fs],stdout=subprocess.PIPE)
    output, stderr = p.communicate()
    output = output.split("\n")[1]
    regex = re.compile(r".*-f\s*(?P<fragsize>\d*)")
    fragsize = int(regex.match(output).group("fragsize"))
    p = subprocess.Popen(["/sbin/dumpfs.ufs", "-f", fs], stdout=subprocess.PIPE)
    output, stderr = p.communicate()
    output = output.split("\n")
    
    free_blocks = list()
    
    for line in output:
        line = line.split("-")
        if len(line) == 2:
            begin, end = line
            begin = int(begin)
            end = int(end)
            begin *= fragsize
            end *= fragsize
            free_blocks.append((begin, end))
        elif len(line) == 1:
            if not line[0]:
                continue
            frag = int(line[0])
            frag *= fragsize
            free_blocks.append((frag,frag))
    return fragsize, free_blocks    



def copy_fs(blklist, fragsize, infile, outfile, start=0, end=None):
    """
    copies used blocks only..
    blklist and fragize returned from get_free_blocks
    """
    #TODO: figure out the math to copy more than <fragsize> bytes at a time
    progress_obj = ProgressDummy()
    parent_th = threading.currentThread()
    
    if os.isatty(sys.stdout.fileno()):
        progress_th = threading.Thread(
            name="progress_th",
            target=_progress_output,
            args=(progress_obj, parent_th)
        )
        
        progress_th.start()
    if end is None:
        infile.seek(0, os.SEEK_END)
        end = infile.tell()
    
    progress_obj.total = end
    
    
    infile.seek(start, os.SEEK_SET)
    nullbyte = '\0'
    while infile.tell() < progress_obj.total:
        progress_obj.cur = infile.tell()
        if len(blklist) >= 1:
            assert len(blklist[0]) == 2
            barrier = blklist.pop(0)
            #print barrier
        else:
            barrier = (progress_obj.total * 2, progress_obj.total * 2)
        
        while infile.tell() < barrier[0] and infile.tell() < progress_obj.total:
            buf = infile.read(fragsize)
            outfile.write(buf)                
        
        while infile.tell() <= barrier[1] and infile.tell() < progress_obj.total:
            #fd.seek(+fragsize, os.SEEK_CUR)
            tmp_pos = infile.tell()
            infile.read(fragsize)
            diff_pos = infile.tell() - tmp_pos
            outfile.write(nullbyte * diff_pos)

    progress_obj.finished = True
    
if __name__ == "__main__":
    fs = sys.argv[1]
    if len(sys.argv) >= 3:
        outfile = sys.argv[2]
    else:
        outfile = "/dev/null"
    
    infile = open(fs, "rb")
    outfile = open(outfile, "wb")
    
    fragsize, blklist = get_free_blocks(fs)
    copy_fs(blklist, fragsize, infile, outfile)