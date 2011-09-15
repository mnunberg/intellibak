#!/usr/bin/python2.6

import os
import re
from fs import FSType
from fst_defs import defaultFST
import subprocess
import compress
import threading
import time
import lvm
import partition
import fs
import errno
import _loop
import sys
import time
import datetime
import persist
import gzip

class InvalidMountpoint(Exception): pass


def _wrap_prepare_end(f):
    """
    Wrapper function for backup_prepare and backup_end calls
    """
    def f_new(self):
        if self._skipped:
            return
        else:
            f(self)
    return f_new


class BackupGroup(object):
    """
    Contains a list of BackupEntry items which share a common ancesstor
    the aim of a BackupGroup is to allow different BackupEntry objects which
    are of different types and hence different methods to be grouped into a
    single group with a common property. What the BackupGroup does here is equate
    to a disk image or such, in which different partitions are containing different
    filesystems.
    """
    
    def __init__(self, name, is_image=False, group_compression = None,
                 compress_threads=None, compress_level=None, grace_period = 0.0,
                 backup_base = None, **noargs):
        self._name = name
        self.__is_image = bool(is_image)
        if group_compression:
            assert (isinstance(group_compression, compress.CompressObject)
                    or issubclass(group_compression, compress.CompressObject))
        else:
            group_compression = compress.Null
        
        self._group_compression = group_compression
        self._compress_level = compress_level
        self._compress_threads = compress_threads
        
        self._dt_timestamp = datetime.datetime.now()
        self._grace_period = grace_period
        
        if backup_base:
            self.backup_base = backup_base
        
        ###Instance vars
        self.__bu_list = []
        self.__backup_base = False
        self._parent_dev = None
        self._pt_entries = None
        self._persist_format = None
        
        #If we don't need to do anything, e.g. grace period is not up, then we
        #do not need to bother with setting up mappings, snapshots, whatever
        self._skipped = True 

    
    def persist_init(self):
        group_dir = os.path.dirname(self.destpath)
        self.Persist = persist.PersistXML(os.path.join(group_dir, "state.xml"))
        tmp_o = self.Persist.add_bu_group(name=self.name,
                                          _class=self.__class__.__name__,
                                          id=time.mktime(self._dt_timestamp.timetuple()),
                                          child_dir = os.path.basename(self.destpath))
        self.Persist_cur = tmp_o
        
    def _determine_actions(self):
        """
        This is a pre-initialization function. Its job is to check whether there
        is any work to be done for the backup entries. If there is not, it flags
        the _skipped flag as true and also removes our current instance from
        persistent storage
        """
        def within_grace_period(entry):
            if not bool(self._grace_period):
                return False
            
            for instance in self.Persist.get_instances():
                for e in instance.get_entries():
                    if e.name == entry.Persist_cur.name and e.time_end:
                        if float(e.time_end) >= (time.mktime(self._dt_timestamp.timetuple()) -
                                                 self._grace_period) and (e.outfile and instance.child_dir):
                            intended_filename = os.path.join(os.path.dirname(self.destpath),instance.child_dir, e.outfile)
                            if os.path.exists(intended_filename) and e.success:
                                return True
            return False
        
        nbackups = 0
        for e in self.entries:
            if within_grace_period(e):
                e.proceed = False
                e.Persist_cur.remove_self()
            else:
                e.proceed = True
                nbackups += 1
        if nbackups > 0:
            self._skipped = False
            
        else:
            self._skipped = True
            self.Persist_cur.remove_self()
        
    @_wrap_prepare_end
    def backup_prepare(self):
        """
        Creates mappings, and may have additional prerequisites performed
        """
        #Ensure our destination paths exist:        
        for d in (os.path.dirname(self.destpath), self.destpath):
            try:
                os.mkdir(d)
            except OSError as err:
                if err.errno != errno.EEXIST:
                    raise
        
        self._create_mappings()  
    
    @_wrap_prepare_end        
    def backup_end(self):
        """
        Undo mappings and other prerequisites
        """
        self._remove_mappings()
    def backup_start(self):
        if self._skipped:
            return
        #Get parent device size:
        _d = open(self.parent_dev,"rb")
        _d.seek(0, os.SEEK_END)
        dev_size = _d.tell()
        _d.close()
        self.Persist_cur.dev_size = dev_size
        
        tasks = 0
        for entry in self.entries:
            if not entry.proceed:
                print "SKIPPING", entry
                continue
            _now = time.time()            
            p = entry.Persist_cur
            p.time_start = _now
            try:
                entry.backup()
                p.success = True
                
            except Exception as err:
                print "Something happened! saving to XML"
                p.success = False
                raise
            finally:
                p.time_end = time.time()
                self.Persist.save()
        
        if tasks == 0:
            self.Persist_cur.remove_self()
            return
        self.Persist_cur.save()
        self.handle_rest()
        
    def _create_mappings(self):
        """
        Create mappings for partition table entries via dmsetup
        """
        if len(self._pt_entries) == 0:
            return
        print "creating mappings on", self._parent_dev        
        pt_mappings = partition.create_mappings(self.parent_dev, self._pt_entries)
        prefix = pt_mappings[0][0]
        for entry in self.__bu_list:
            entry.set_dmprefix(prefix)
        self.__pt_mappings = pt_mappings
    
    def _remove_mappings(self):
        try:
            partition.remove_mappings(self.__pt_mappings)
        except (NameError, AttributeError):
            pass
        
        
    @property
    def parent_dev(self):
        return self._parent_dev
    @parent_dev.setter
    def parent_dev(self, value):
        self._parent_dev = value
    
    def add_entry(self, bu_entry):
        """
        Add sub-entry, a BackupEntry object
        """
        if not isinstance(bu_entry, BackupEntry):
            raise ValueError("Expected type BackupEntry")
        else:
            self.__bu_list.append(bu_entry)

    @property
    def entries(self): return self.__bu_list
    
    @property
    def name(self): return self._name
    
    @property
    def destpath(self):
        str_time = "-".join([str(i) for i in (self._dt_timestamp.year, self._dt_timestamp.month, self._dt_timestamp.day)])
        
        if self.backup_base:
            return os.path.join(self.backup_base, self.name, str_time)
        else:
            return os.path.join("NO_BU_VOL", self.name, str_time)
    @property
    def backup_base(self): return self.__backup_base
    @backup_base.setter
    def backup_base(self, value):
        """
        value: first argument is whether to force the path even if it is not a
        mountpoint, the rest of  the arguments are path components
        """
        if len(value) < 2:
            raise ValueError()
            
        force, mountpoint = value[:2]
        args = value[2:]
        
        if not force:
            p = subprocess.Popen(['/bin/mountpoint', '-q', mountpoint])
            if p.returncode != 0:
                raise InvalidMountpoint(mountpoint)
        
        if not os.path.exists(mountpoint):
            raise ValueError("Destination basepath does not exist!")
        self.__backup_base = os.path.join(mountpoint, *args)
    
        
    def create_backup_entries(self, fst_list):
        """
        Scan the BackupGroup.parent_dev for sub-entries and add those to the bu_list
        """
        pt_entries = partition.get_pt_entries(self.parent_dev)
        if pt_entries:
            self._pt_entries = pt_entries
            
            for index, entry in enumerate(pt_entries, start=0):
                fst = fs.get_fstype(self.parent_dev, fst_list,
                                    seek = (entry.start * 512))
                if not fst:
                    fst = defaultFST

                if fst.name.lower().strip() == "ufs_bsd"  and entry.name.endswith("p1"):
                    print "Skipping dummy BSD Partition", entry.name
                    pt_entries.pop(index)
                    continue
                
                #Create BackupEntry in persistent storage:
                e_persist = self.Persist_cur.add_entry(mapping_start=entry.start,mapping_end=entry.end)
                
                tmp = BackupEntry(entry.name, fst, self.destpath, needs_mapping = True,
                                  compressor=self._group_compression,
                                  compress_threads = self._compress_threads,
                                  compress_level = self._compress_level,
                                  persist_obj = e_persist)
                self.add_entry(tmp)

        else:
            self._pt_entries = []
            fst = fs.get_fstype(self.parent_dev,fst_list)
            if fst:
                e_persist = self.Persist_cur.add_entry()
                tmp = BackupEntry(self.name, fst, self.destpath, needs_mapping = False,
                                  compressor = self._group_compression,
                                  compress_threads = self._compress_threads,
                                  compress_level = self._compress_level, persist_obj = e_persist)
                self.add_entry(tmp)

        
        #Having added all the entries, check if any of them actually need backup,
        #if so, this function handles all the entries to see whether it needs
        #processing, and if none of them need processing the entire BackupGroup
        #is skipped, no snapshots are created, no mappings are made etc.
        self._determine_actions()        
        
    def handle_rest(self):
        """
        This is meant to copy the rest of the media which was not yet handled
        by any of the fscopy  modules. This includes the partition table itself,
        and any associated undetected filesystems. This uses 512 byte blocks and
        will take time. I'm assuming sane configurations here
        """
        if len(self._pt_entries) == 0:
            return
        
        chunkdir  = os.path.join(self.destpath, "extra_chunks")
        try:
            os.mkdir(chunkdir)
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise

        skip_regions = []
        for e in self._pt_entries:
            if e.start and e.end:
                skip_regions.append((int(e.start), int(e.end)))
        
        skip_regions.sort(key=lambda x: x[0])
        dev_size = int(self.Persist_cur.dev_size)
        
        if skip_regions[0][0] != 0:
            last_region_end = 0
        else:
            last_region_end = skip_regions[0][1]
            skip_regions.pop(0)
        
        try:
            infile = open(self.parent_dev, "rb")
            outfile = None
            
            for r in skip_regions:
                print "last_region_end:", last_region_end, "r:", r
                outfile = gzip.open(os.path.join(chunkdir, "{0}-{1}.gz".format(last_region_end,r[0])),"wb")
                infile.seek(last_region_end*512, os.SEEK_SET)
                print infile.tell() / 512
                while infile.tell() < r[0]*512:
                    outfile.write(infile.read(512))
                outfile.close()
                last_region_end = r[1]
        finally:
            if infile:
                infile.close()
            if outfile:
                outfile.close()
            
        
    def __hash__(self):
        return hash(self.name)
    
    def __iter__(self):
        return iter(self.entries)
        
    def __str__(self):
        return "Name = {0.name}, Entries: {1}".format(self, len(self.entries))
    def __len__(self):
        return len(self.entries)
    def __nonzero__(self):
        return True
    


class ImageBackupGroup(BackupGroup):
    def __init__(self, *args, **kwargs):
        """
        self, name, is_image=False, group_compression = None,
        compress_threads=None, compress_level=None
        """
        super(ImageBackupGroup,self).__init__(*args, **kwargs)
        self._file_name = self.name
        self.parent_dev = self._file_name
        self._name = os.path.basename(self.name)
        
    @_wrap_prepare_end
    def backup_prepare(self):
        loop_dev = _loop.make_loop(self.parent_dev)
        if not loop_dev:
            raise Exception("GAH")
        self.parent_dev = loop_dev
        self._loop_dev = loop_dev
        
        
        try:
            os.mkdir(self.destpath,0755)
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise
            
        super(ImageBackupGroup, self).backup_prepare()
    
    @_wrap_prepare_end
    def backup_end(self):
        super(ImageBackupGroup,self).backup_end()
        _loop.remove_loop(self._loop_dev)
        self._parent_dev = None
    
    @property
    def parent_dev(self):
        return self._parent_dev
    
    @parent_dev.setter
    def parent_dev(self, value):
        if not os.path.exists(value):
            raise ValueError("Image does not exist!")
        self._parent_dev = value


class LVMBackupGroup(BackupGroup):
    #name == lv_name -- note
    def __init__(self, name, vg_name=None, snapshot_prefix="BU_SNAPSHOT_", **kwargs):
        """
        kwargs == rest of args for BackupGroup class
        """
        if not vg_name:
            raise ValueError("Must have vg_name")
            
        name = os.path.basename(name)
        super(LVMBackupGroup, self).__init__(name, **kwargs)
        self.__lv_name = name
        self.__vg_name = vg_name
        if '/' in snapshot_prefix:
            raise ValueError("snapshot prefix must contain only legal file chars")
        self.__snapshot_prefix = snapshot_prefix
        self._parent_dev = os.path.join('/dev/', vg_name, name)
        
    @property
    def lv_name(self): return self.__lv_name
    @property
    def snapshot_prefix(self): return self.__snapshot_prefix
    @property
    def vg_name(self): return self.__vg_name
    
    @_wrap_prepare_end
    def backup_prepare(self):
        if self._skipped:
            return
        
        self.__create_snapshot()
        self.parent_dev = os.path.join('/dev/',
                                         self.vg_name, self.snapshot_prefix + self.lv_name)
        
        for p in self._pt_entries:
            p.name = self.snapshot_prefix + os.path.basename(p.name)
        super(LVMBackupGroup, self).backup_prepare()
    
    @_wrap_prepare_end  
    def backup_end(self):
        if self._skipped:
            return
        super(LVMBackupGroup, self).backup_end()
        self.__remove_snapshot()

    
    def __create_snapshot(self):
        lvm.create_snapshot(self.lv_name, self.vg_name, prefix=self.snapshot_prefix)
        for entry in self.entries:
            entry.abs_snapshot_prefix = os.path.join('/dev',
                                                 self.vg_name,
                                                 self.snapshot_prefix)
        
    def __remove_snapshot(self):
        lvm.remove_snapshot(os.path.join("/dev", self.vg_name,
                                         self.snapshot_prefix + self.lv_name))
    

class BackupEntry(object):
    """
    Mid-level class to store information about the path and type of a
    backup source
    args: path, the path to the original source volume; type, FSType object;
        needs_mapping, whether the path exists or must be created via devmapper;
        compressor, CompressObject; backupmethod, block copy method from fs module
    """
    
    def __init__(self, name, type, destpath,
                 needs_mapping=False, compressor=None, backupmethod=None,
                 compress_threads=None, compress_level=None, persist_obj = None):
        
        #Compression opts
        if compressor:
            if not (issubclass(compressor, compress.CompressObject)):
                raise TypeError("Expected CompressObject, not {0}".format(type(compressor)))
        self.__compressor =  compressor
        self._compress_level = compress_level
        self._compress_threads =  compress_threads
        
        
        #Parameters for backup path
        if not destpath.startswith("/"):
            raise ValueError("Destination path must be absolute!")
        self.__destpath = destpath
        self.__name = os.path.basename(name)
        self.__needs_mapping = needs_mapping #I don't  think this is being used for anything
        
        self.__type = type
        if not isinstance(self.__type, FSType):
            raise TypeError("Expected FSType")

        if backupmethod:
            self._backup_method = backupmethod
        else:
            self._backup_method = self.type.backupmethod
        
        #For persistent storage
        if persist_obj:
            persist_obj.name = self.name
            persist_obj.type = self.type.name
            persist_obj.compression = self.__compressor
        self.Persist_cur = persist_obj
        
        ####Instance vars:
        self.__dmprefix = False
        self.__abs_snapshot_prefix = False
        self.proceed = True #A per-entry flag, may be set to false. Just for external convenience

        
        
    @property
    def needs_mapping(self): return self.__needs_mapping
    @property
    def name(self): return self.__name
    @property
    def type(self): return self.__type

    @property
    def abs_snapshot_prefix(self):
        return self.__abs_snapshot_prefix
    
    @abs_snapshot_prefix.setter
    def abs_snapshot_prefix(self, value):
        if not value.startswith('/'):
            raise ValueError("snapshot prefix must be absolute")
        self.__abs_snapshot_prefix = value
        
    def set_dmprefix(self, dmprefix="BU_SPECIAL_"):
        """
        Sets the devicemapper prefix to be used. The prefix is used in order to
        make the volume stand out in error messages and such. This function
        provides the actual path to the device, the prefix is obtained from
        elsewhere (see BackupGroup functions), and is merely set here to aid in
        the backup() function
        """
        if dmprefix.startswith('/'):
            raise ValueError("illegal character in prefix")
        self.__dmprefix = dmprefix
        
    @property
    def destpath(self):
        if self.__destpath:
            return os.path.join(self.__destpath, self.name)
        else:
            return None

    @property
    def snapshot_path(self):
        if self.__needs_mapping:
            if self.abs_snapshot_prefix:
                _snapshot_prefix = os.path.basename(self.abs_snapshot_prefix)
            else:
                _snapshot_prefix = ""
                
            return "/dev/mapper/" + self.__dmprefix + _snapshot_prefix + self.name
        else:
            return self.abs_snapshot_prefix + self.name
    
        
    def backup(self, **kwargs):
        """
        This function ties everything together; it uses a block copy function
        whose job it is to copy only the RELEVANT blocks from the media, then
        this is piped to a compression function, and finally written to the
        destination media
        """
        if not self.destpath:
            print ("No destination path set")
            return
        
        if not os.path.exists(self.snapshot_path):
            raise OSError(
                "Snapshot path: {0} does not exist!".format(self.snapshot_path))
                    
        infile = self.snapshot_path
        print "INFILE:", infile
        
        outfile_name =  ".".join([self.destpath,self._backup_method.suffix,self.__compressor.suffix])
        
        
        ###LVM###
        if self.type.name.upper() == "LVM" and "LVM" in self._backup_method.__name__.upper():
            
            self.Persist_cur.outfile = os.path.basename(self.destpath)
            self._backup_method(infile, self.destpath,
                                 compressor = self.__compressor,
                                 level = self._compress_level,
                                 threads = self._compress_threads)
            
            #Let LVM handle its own thing and return. Nothing left for us to do here
            return
        ###END_LVM###
        
        #Set actual file name for logs:
        self.Persist_cur.outfile = os.path.basename(outfile_name)
        
        #Open outfile:
        print "OUTFILE:",  outfile_name
        outfile = open(outfile_name, "wb")

        #Prepare piplines
        pipeline_read, pipeline_write = os.pipe()
        pipeline_read = os.fdopen(pipeline_read, "rb")
        pipeline_write = os.fdopen(pipeline_write, "wb")
        
        #Prepare threads
        block_copy_th = threading.Thread(
            target = self._backup_method,args=(infile, pipeline_write),name="block_copy")
        compress_th = threading.Thread(
            target = self.__compressor(level=self._compress_level,
                                       threads = self._compress_threads).compress,args=(pipeline_read, outfile),name="compress")
        
        #Start threads
        block_copy_th.start()
        compress_th.start()
        
        #Wait and close...
        block_copy_th.join()
        pipeline_write.close()
        
        compress_th.join()
        pipeline_read.close()
        
    def __str__(self):
        _type  = self.type if self.type else "<UNKNOWN>"
        _compression = self.__compressor if self.__compressor else "none"
        _backup_method = self._backup_method.__name__ if self._backup_method else "none"
        _mapping_status = "mapped" if self.needs_mapping else "unmapped"
        _compress_level = str(self._compress_level) if self._compress_level else "<DEFAULT>"
        _compress_threads = str(self._compress_threads) if self._compress_threads else "<DEFAULT>"
        return ("name = {name}, type = {type}, "
                "compression = [type = {compression}, level = {compress_level}, threads = {compress_threads}], "
                "copy = {copy}, {mapping_status}").format(
            name = self.name, type = _type, compression = _compression,
            compress_threads = _compress_threads, compress_level = _compress_level,
            copy = _backup_method, mapping_status = _mapping_status)
        
    def __hash__(self): return self.path.__hash__()
    