#!/usr/bin/env python2.6

class LVMNestedError(Exception): pass

from fscopy import ImageDump, FSCopy
from subprocess import Popen, PIPE
from fs import get_fstype
from glob import glob

import lvm
import os
import errno
import threading

class LVMNestedBackup(FSCopy):
    """
    Handles nested LVM volumes, intended for use when the LVM  VG is normally not
    active
    """
    suffix = ""
    def fscopy(self, infile, outpath, **kwargs):
        #Import this list at call-time rather than at initialization:
        from fst_defs import fst_list

        #Let's make some assumptions:
        #(1): nested LVM will contain only Linux filesytems and possibly swap space
        #(2): we are mapped and infile is an actual device node
        #(3): We are not part of a mirrored/striped setup
        #(4): No snapshot is needed. Assume we are being fed a mapping which is already
        #     part of a snapshot
        ##############
        #Steps:
        # I. First have LVM pick up the volume group
        # II. Assume our filesystems are either mountable Linux or swap.
        # III. We need a single outfile, we will  make a single tar archive containing
        #      each filesystem, the LVM metadata (first 384 blocks of the volume), and
        #      a mapping (in simple key=value format) of which files within the tar
        #      archive belong to which LVM LV.. or better yet, simply name the internal
        #      files as the LV  itself
        
        #After horribly hacking common.py to give us a compress mechanism and also pass us
        #actual paths and not file objects, we have a lot more to work with
        
        #Determine the VG name:
        p = Popen(['pvs', infile, '--noheadings', '-o', 'vg_name'], stderr=PIPE, stdout=PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise LVMNestedError(stderr)
        vg_name = stdout.strip()
        
        self._persistent_vars['vg_name'] = vg_name
        
        #Activate our VG
        lvm.vg_activate(vg_name)
        
        try:
            os.mkdir(outpath)
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise

        for bdev in glob(os.path.join('/dev', vg_name, '*')):
            fst = get_fstype(bdev, fst_list)
            if not fst or fst.name.lower() == "lvm":
                backupmethod = ImageDump
            else:
                backupmethod = fst.backupmethod
            
            #Set up names
            outname = ".".join([os.path.basename(bdev), backupmethod.suffix, kwargs['compressor'].suffix])
            outfile = open(os.path.join(outpath, outname), "wb")
            
            #Establish pipeline:
            pipeline_read, pipeline_write = os.pipe()
            pipeline_read = os.fdopen(pipeline_read, "r")
            pipeline_write = os.fdopen(pipeline_write, "w")
            
            #Establish compression and fscopy threads:
            compress_th = threading.Thread(
                name = "compress_lvm",
                target=kwargs['compressor'](level=kwargs['level'],threads=kwargs['threads']).compress,
                args = (pipeline_read, outfile))
            
            fscopy_th = threading.Thread(name="fst_bumethod_lvm", target=backupmethod, args=(bdev,pipeline_write))
            
            compress_th.start()
            fscopy_th.start()
            
            compress_th.join()
            fscopy_th.join()
        
        #Copy LVM metadata:
        metadata=open(infile,"rb").read(512*384)
        open(os.path.join(outpath, "lvm_metadata"),"wb").write(metadata)
    
    def cleanup(self):
        lvm.vg_deactivate(self._persistent_vars['vg_name'])