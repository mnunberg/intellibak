#!/usr/bin/python2.6


# Struct from C code:
#struct loop_info64 {
#	unsigned long long	lo_device;
#	unsigned long long	lo_inode;
#	unsigned long long	lo_rdevice;
#	unsigned long long	lo_offset;
#	unsigned long long	lo_sizelimit; /* bytes, 0 == max available */
#	unsigned int		lo_number;
#	unsigned int		lo_encrypt_type;
#	unsigned int		lo_encrypt_key_size;
#	unsigned int		lo_flags;
#	unsigned char		lo_file_name[LO_NAME_SIZE];
#	unsigned char		lo_crypt_name[LO_NAME_SIZE];
#	unsigned char		lo_encrypt_key[LO_KEY_SIZE];
#	unsigned long long	lo_init[2];
#};
#usage in C code:
#        if (ioctl(fd, LOOP_GET_STATUS64, &loopinfo64) == 0) {
#...

class LoopError(Exception):  pass

import os
import struct
import errno

from fcntl import ioctl
from collections import namedtuple
from glob import glob
from subprocess import Popen, PIPE



LO_CRYPT_NONE = 0
LO_CRYPT_XOR = 1
LO_CRYPT_DES = 2
LO_CRYPT_CRYPTOAPI = 18

LOOP_SET_FD = 0x4C00
LOOP_CLR_FD = 0x4C01
LOOP_SET_STATUS	= 0x4C02
LOOP_GET_STATUS	= 0x4C03
LOOP_SET_STATUS64 = 0x4C04
LOOP_GET_STATUS64 = 0x4C05

LO_NAME_SIZE = 64
LO_KEY_SIZE = 32


loop_info64 = struct.Struct("<5Q"
                            "4I"
                            "{lo_name_size}s"
                            "{lo_name_size}s"
                            "{lo_key_size}s"
                            "2L".format(
    lo_name_size = LO_NAME_SIZE, lo_key_size = LO_KEY_SIZE))

l = ((5*[0L])   +
    (4*[0])     +
    (3*[''])    +
    (2*[0L])
    )
    
LoopInfo = namedtuple('LoopInfo',
                      'lo_device lo_inode lo_rdevice lo_offset lo_sizelimit '
                      'lo_number lo_encrypt_type lo_encrypt_key_size lo_flags '
                      'lo_file_name lo_crypt_name lo_encrypt_key lo_init')

s = loop_info64.pack(*l)


def loop_info(path):
    global s
    fp = open(path, "rb")
    try:
        s = ioctl(fp,LOOP_GET_STATUS64, s)
    except IOError as err:
        if err.errno == errno.ENXIO:
            return None
        raise err
    
        
    fp.close()
    tmp =  loop_info64.unpack(s)
    lo_info = LoopInfo(
        lo_device = tmp[0:1][0],
        lo_inode = tmp[1:2][0],
        lo_rdevice = tmp[2:3][0],
        lo_offset = tmp[3:4][0],
        lo_sizelimit = tmp[4:5][0],
        lo_number = tmp[5:6][0],
        lo_encrypt_type = tmp[6:7][0],
        lo_encrypt_key_size = tmp[7:8][0],
        lo_flags = tmp[8:9][0],
        lo_file_name = tmp[9:10][0],
        lo_crypt_name = tmp[10:11][0],
        lo_encrypt_key = tmp[11:12][0],
        lo_init = tmp[12:14]
        )
    return lo_info
    


def make_loop(f_path):
    #Get free loop device:
    free_dev = None
    for loop_dev in glob("/dev/loop*"):
        if loop_info(loop_dev) is None:
            free_dev = loop_dev
            break
        
    if not free_dev:
        return None
    
    p = Popen(['/sbin/losetup', '-r', free_dev, f_path], stderr=PIPE)
    stdout, stderr = p.communicate()
    if p.returncode !=0:
        raise LoopError(stderr)
    return free_dev

def remove_loop(dev):
    p = Popen(['/sbin/losetup', '-d', dev], stderr=PIPE)
    stdout, stderr = p.communicate()
    if p.returncode !=0:
        raise  LoopError(stderr)
    


#def loop_attach(path, offset=0, mode = os.O_RDONLY):
#    ###BROKEN
#    global loop_info64
#    global s
#    
#    info=LoopInfo(*[0]*13)
#    
#    #Set non integer values
#    info._replace(lo_file_name=path, lo_crypt_name="", lo_encrypt_key="",
#                  lo_offset=offset, lo_init=[0,0])
#    
#    #Get free loop device:
#    free_dev = None
#    for loop_dev in glob("/dev/loop*"):
#        if loop_info(loop_dev) is None:
#            free_dev = loop_dev
#            break
#        
#    if not free_dev:
#        return None
#    
#    ffd = os.open(path, mode)
#    fd = os.open(free_dev, os.O_RDWR)
#    print len(info)
#    loop_info64_st = loop_info64.pack(*info)
#    ioctl(fd, LOOP_SET_FD, ffd)
#    ioctl(fd, LOOP_SET_STATUS64, loop_info64_st)
    
#if __name__ == "__main__":
#    import sys
#    if len(sys.argv) == 2:
#        info = loop_info(sys.argv[1])
#        if not info:
#            sys.exit(errno.ENXIO)
#        for i in info._asdict().items():
#            key, value = i
#            print "{0} = {1}".format(key, value)
#        loop_attach(sys.argv[1])