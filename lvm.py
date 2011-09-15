#!/usr/bin/python2.6

import subprocess
import os

class LVMError(Exception): pass
class SnapshotError(LVMError): pass

def create_snapshot(lv_name, vg_name, prefix="BU_SNAPSHOT_", size="1G"):
    print "lvm: Creating snapshot: {0}/{1}".format(vg_name, lv_name)
    p = subprocess.Popen(
        ["/sbin/lvcreate", "-s", "-n" + prefix + os.path.basename(lv_name),
         "-L" + size, os.path.join("/dev", vg_name, lv_name)],
        stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    
    stdout, stderr = p.communicate()
    
    if p.returncode != 0:
        raise SnapshotError(stderr)
    
    return True

def remove_snapshot(lv_snapshot_name):
    print "lvm: Removing snapshot {0}".format(lv_snapshot_name)
    p = subprocess.Popen(
        ["/sbin/lvremove", '-f', lv_snapshot_name],
        stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise SnapshotError(stderr)
    
    return True

class VGChangeError(Exception): pass

def vg_activate(vg_name):
    os.chdir("/")
    p = subprocess.Popen(['/sbin/vgchange', '-a', 'y', vg_name],
        stderr = subprocess.PIPE)
    stdout, stderr = p.communicate()
    
    if p.returncode != 0:
        raise VGChangeError(stderr)
    
    return True

def vg_deactivate(vg_name):
    os.chdir("/")
    p = subprocess.Popen(['/sbin/vgchange', '-a', 'n', vg_name],
        stderr = subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise VGChangeError(stderr)
    return True

def get_lvs(vg_name):
    p = subprocess.Popen(['lvs', vg_name, '-o', 'lv_name', '--noheadings'],
        stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise LVMError(stderr)

    return [lvn.strip() for lvn in stdout.split("\n") if lvn.strip()]
    