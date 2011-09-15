#!/usr/bin/python2.6

LICENSE="""
This is part of a backup system framework in python
Copyright M. Nunberg

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import sys
import re
import cStringIO
import ConfigParser
import os
from collections import namedtuple
import time

ConfigKeys = namedtuple("ConfigKeys", 'config_name internal_name type default')
ConfVal = namedtuple("KVPair", "value is_default")
#Dummy container for persistent config objects
class ConfigContainer(object): pass

class GracePeriodCfg(float):
    """
    Wrapper class for time config value
    """
    MINUTE = 60
    HOUR = MINUTE * 60
    DAY = HOUR * 24
    WEEK = DAY * 7
    MONTH = DAY * 30
    calc_table = [
        ("minute", MINUTE),
        ("hour", HOUR),
        ("day", DAY),
        ("week", WEEK),
        ("month", MONTH)
    ]
    
    def __new__(self,timecfgfmt):
        if not timecfgfmt:
            return float.__new__(self,0.0)
        _time = 0.0
        for time_element in timecfgfmt.split("+"):
            amount, unit = time_element.split()
            
            unit = unit.lower().strip()
            amount = int(amount)
            
            for s, nsecs in self.calc_table:
                if s in unit:
                    _time += nsecs * amount
        return float.__new__(self, _time)


class ExcludeLVCfg(list):
    def __init__(self, s):
        if not s:
            super(ExcludeLVCfg, self).__init__()
            return
        
        super(ExcludeLVCfg, self).__init__([lv.strip() for lv in s.split(",")])

def __mk_ck_list(opt_list):
    ck_list = []
    for entry in opt_list:
        assert len(entry) == 4
        config_name, internal_name, _type, default = entry
        if not internal_name:
            internal_name = config_name
        ck_list.append(
            ConfigKeys(config_name, internal_name, _type, default))
    return ck_list


def group_config():
    opt_list = [
        #config name | internal name | type | default
        ("dev", None, "str", None),
        ("compression", None, "str", "gzip"),
        ("type", None, "str", None),
        ("vg", None, "str", None),
        ("file", None, "str", None),
        ("grace_period", None, "GracePeriodCfg", "2 weeks")
    ]
    
    return __mk_ck_list(opt_list)
    
def gloabls_config():
    opt_list = [
        ("dest_path", None, "str", None),
        ("compression", "global_compression", "str", "Null"),
        ("compress_level", None, "int", 5),
        ("compress_threads", None, "int", os.sysconf("SC_NPROCESSORS_ONLN")),
        ("grace_period", "global_grace_period", "GracePeriodCfg", "2 weeks")
    ]
    return __mk_ck_list(opt_list)



def vgprobe_config():
    opt_list = [
        ("vg", None, "str", None),
        ("exclude", None, "ExcludeLVCfg", None)
    ]
    return __mk_ck_list(opt_list)
    

def set_kv_pairs(fp, ck_list, section_name, kv_obj):
    """
    fp is a StringIO object which contains a single section with key/value
    pairs, ck_list is a ConfigKeys namedtuple which contains the internal name,
    config name, and the type to check for, kv_obj is an object which supports
    the syntax of obj.${ck_list[item].internal_name} = foo, This is forced
    because any variable delcared internally is out of scope from the rest of
    the code
    """
    config = ConfigParser.SafeConfigParser()
    fp.seek(0)
    config.readfp(fp)
    for key in ck_list:
        if key.internal_name is None:
            key = ConfigKeys(key.config_name, key.config_name, key.type, key.default)
        try:
            exec "val=config.get('{section}', '{keyname}')".format(
                section = section_name, keyname = key.config_name)
            tmp_confval = ConfVal(value=val, is_default=False)
            
        except ConfigParser.NoOptionError as err:
            tmp_confval = ConfVal(value = key.default, is_default=True)
            
        try:
            exec("_copy_type = {_type}(tmp_confval.value)".format
                 (_type = key.type))
        except ValueError  as err:
            print err
            continue
        
        exec("kv_obj.{kv_internal} = "
             "ConfVal(value=_copy_type, is_default=tmp_confval.is_default)".format(
                kv_internal = key.internal_name))
        
    return ck_list
    
        
def get_sections(fp, section_name):
    """
    prepares sections with identical names for the ConfigParser module.
    -> [cStringIO.StringIO("[section]..keyvalues"),...]
    """
    regex = re.compile(
        r"(?P<keyvals>\[{0}\][^[]*)".format(section_name), re.I|re.M|re.S)    
    fp.seek(0)
    buf = fp.read()
    
    config_list = []

    match_list = regex.finditer(buf)
    if not match_list:
        return []
        
    for match in match_list:
        tmp = cStringIO.StringIO(match.group("keyvals"))
        config_list.append(tmp)
  
    return config_list
