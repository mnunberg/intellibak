#!/usr/bin/python2.6

import configfile
import sys
import common
import fscopy
import compress

from optparse import OptionParser
from fst_defs import fst_list

def _get_cli_opts():
    """
    -> opts, args
    """
    parser = OptionParser()
    parser.add_option("-d", "--destination",  default="/<path>",
                      help="destination backup mountpoint", dest="bu_dest")
    parser.add_option("-s", "--source",
                      help="backup source VG", dest="vg_src")
    parser.add_option("-c", "--config",
                      help="configuration file", dest="config_file")
    parser.add_option("-n", "--dry-run", dest="dry_run", action="store_true", default=False,
                      help="Don't backup or create any mappings. Just test detection code")
    return parser.parse_args()



def get_global_params(fp):
    """
    -> configfile.ConfigContainer()
    """    
    global_opts = configfile.ConfigContainer()
    
    #Get global parameters
    global_section = configfile.get_sections(fp, 'global')[0]
    global_ck_list = configfile.gloabls_config()
    configfile.set_kv_pairs(global_section,global_ck_list,'global',global_opts)
    return global_opts



def get_groups_from_config(fp, global_opts):
    """
    -> list of  BackupGroup objects
    """
    #our main list, which we return:
    bg_list = []
    
    #Some name bindings:
    compress_level = global_opts.compress_level.value
    compress_threads = global_opts.compress_threads.value
    dest_path = global_opts.dest_path.value
    global_compression  = global_opts.global_compression
    global_grace_period = global_opts.global_grace_period
    
    #Get keys and actual [group] text sections from the config file
    s_list = configfile.get_sections(fp, 'group')
    confkeys = configfile.group_config()
    
    for s in s_list:
        #Get options from the config file
        bg_opts = configfile.ConfigContainer()
        configfile.set_kv_pairs(s, confkeys, 'group', bg_opts)
        
        #Verify the config file has options we need
        try:
            (bg_opts.dev,bg_opts.type,bg_opts.compression)
        except (NameError, AttributeError) as err:
            print sys.exc_info()[2]
            continue
        
        #kwargs to be passed to class constructors
        kwargs = dict()
        
        #Get group-specific compression if specified
        if not global_compression.is_default and bg_opts.compression.is_default:
            compression = global_compression.value
        else:
            compression = bg_opts.compression.value
        compression = compress.get_compressor(compression)
        
        #Update kwargs with compression params
        kwargs.update({"group_compression":compression, "compress_threads": compress_threads, "compress_level": compress_level})

        #get group-specific grace period parameters, if specified
        if not global_grace_period.is_default and bg_opts.grace_period.is_default:
            grace_period = global_grace_period.value
        else:
            grace_period = bg_opts.grace_period.value
        kwargs.update({"grace_period":grace_period})
        
        tmp = None
        #LVM-based Logical Volume
        if bg_opts.type.value.upper() == "LVM":
            tmp = common.LVMBackupGroup(bg_opts.dev.value, vg_name = bg_opts.vg.value, **kwargs)
        #File-based image
        elif bg_opts.type.value.upper() == "IMAGE":
            tmp = common.ImageBackupGroup(bg_opts.file.value, **kwargs)
        #Possibly commented-out entry
        else:
            continue
        bg_list.append(tmp)
        
        try:
            tmp.backup_base = (True, dest_path)
        except ValueError as err:
            print sys.exc_info()[2]
            raise ValueError("Missing or inappropriate dest path in global config")
    
    return bg_list
        

def get_groups_from_vg(fp, global_opts):
    import lvm
    confkeys = configfile.vgprobe_config()
    s_list = configfile.get_sections(fp, 'vgprobe')
    
    bugroup_constructor_params = {
        "group_compression": compress.get_compressor(global_opts.global_compression.value),
        "compress_threads": global_opts.compress_threads.value,
        "compress_level": global_opts.compress_level.value,
        "grace_period": global_opts.global_grace_period.value
    }
    bg_list = []
    
    for s in s_list:
        vgp_opts = configfile.ConfigContainer()
        configfile.set_kv_pairs(s, confkeys, 'vgprobe', vgp_opts)
        if not hasattr(vgp_opts, 'vg'):
            print "Hrrm.. no VG"
            continue
        if vgp_opts.vg.is_default:
            continue
        
        exclude = []
        if vgp_opts.exclude.value:
            exclude.extend(vgp_opts.exclude.value)
        
        lvs = lvm.get_lvs(vgp_opts.vg.value)
        for lv in lvs:
            if lv in exclude:
                continue
            tmp = common.LVMBackupGroup(lv, vg_name=vgp_opts.vg.value,**bugroup_constructor_params)
            tmp.backup_base = (True, global_opts.dest_path.value)
            bg_list.append(tmp)
    return bg_list

if __name__ == "__main__":
    opts, args = _get_cli_opts()
    cfp = None
    if opts.config_file:
        cfp = open(opts.config_file, "r")
    if not opts.config_file:
        sys.exit(2)
        
    global_opts = get_global_params(cfp)
    bg_list = get_groups_from_config(cfp, global_opts)
    bg_list.extend(get_groups_from_vg(cfp, global_opts))
    
    for bugroup in bg_list:
        bugroup.persist_init()
        
        bugroup.create_backup_entries(fst_list)
        for e in bugroup.entries:
            e.backup_base = (True, opts.bu_dest)
        print bugroup
        
        if opts.dry_run:
            bugroup.print_xml()
            for entry in bugroup:
                print "\t{0}".format(entry)
                    
        print "Preparing Backup..."
        bugroup.backup_prepare()
        
        try:
            bugroup.backup_start()
        finally:
            bugroup.backup_end()