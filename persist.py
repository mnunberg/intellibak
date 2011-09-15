#!/usr/bin/env python2.6
"""
These objects are meant to act as a neutral API for the backup system. Currently
only XML is implemented but it should not take much work for other backends.
"""

from xml.etree import ElementTree
#import lxml.etree as ElementTree
from xml.dom.minidom import parseString
from collections import defaultdict

import os
from contrib import xmlpp


class GroupOverlay(object): pass
class EntryOverlay(object): pass

class PersistObject(object):
    """
    Skeleton object to be used by subclasses; main code will use roughly this
    API, and should be agnostic to any backend
    """
    def __init__(self, source):
        pass
    def add_bu_group(self,**kwargs):
        pass
    def get_instances(self):
        pass
    def save(self, outfile=None):
        pass
    def backup(self, backup_location):
        pass
    def close(self):
        pass


xml_strings = dict()
xml_strings['root'] = "instances"
xml_strings['backup_group'] = "backup_group_instance"
xml_strings['backup_entry'] = "backup_entry"

def _stringify_kw(d):
    for key, value in d.items():
        d[key] = str(value)

class XMLPersistError(Exception): pass

class XMLObj(object):
    """
    Base object which provides python object notation for main code. foo.bar = "hello"
    will actually write "hello" somewhere in XML, and s = foo.bar will get the value of
    bar from XML
    """
    def __init__(self, element, parent):
        """
        Element is an etree.ElementTree.Element object, root is the immediate
        parent node
        """
        super(XMLObj, self).__setattr__('element', element)
        super(XMLObj, self).__setattr__('parent', parent)
        
    def __getattribute__(self, name):
        try:
            return super(XMLObj, self).__getattribute__(name)
        except AttributeError:        
            return self.element.get(name)
            
    def __setattr__(self, name, value):
        self.element.set(name, str(value))
        
    def remove_self(self):
        """
        Remove current item from persistent storage
        """
        self.parent.remove(self.element)

class XMLEntryOverlay(XMLObj): pass

class XMLGroupOverlay(XMLObj):
    
    def add_entry(self, **kwargs):
        """
        add BackupEntry
        """
        _stringify_kw(kwargs)
        tmp = ElementTree.Element(xml_strings['backup_entry'], **kwargs)
        self.element.append(tmp)
        return XMLEntryOverlay(tmp,self.element)
        
    def get_entries(self):
        return [XMLEntryOverlay(e,self.element) for e in self.element.findall(xml_strings['backup_entry'])]
        
class PersistXML(PersistObject):
    def __init__(self, statefile):
        self._statefile = statefile
        if os.path.exists(statefile):
            root = ElementTree.parse(statefile).getroot()
        else:
            root = ElementTree.Element(xml_strings['root'])
        self._root = root
    
    def add_bu_group(self, **kwargs):
        """
        kwargs will store the values as strings into XML with the keys as attributes
        -> XMLGroupOverlay object which supports python object syntax
        """
        _stringify_kw(kwargs)
        
        bu_group = ElementTree.Element(xml_strings['backup_group'], **kwargs)
        self._root.append(bu_group)
        return XMLGroupOverlay(bu_group, self._root)
    def get_instances(self):
        return [XMLGroupOverlay(e,self._root) for e in self._root.findall(xml_strings['backup_group'])]
    
    def save(self, filename=None):
        if not filename:
            filename = self._statefile
        open(filename, "w").write(xmlpp.get_pprint(ElementTree.tostring(self._root)))