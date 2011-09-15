#!/usr/bin/env python2.6

from collections import defaultdict
import sys
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
        except self.exception_list as err:
            #print sys.exc_info()[2]
            #raise FSCopyError(err)
            raise
        finally:
            self.cleanup()
    __init__ = __call__
