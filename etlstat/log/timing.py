"""
    Class to handle time logging

    Date:
        September 2017

    Authors:
        emm13775

    Version:
        0.1

    Notes:

"""

import time
import logging as log

log.basicConfig(level=log.INFO)
LOGGER = log.getLogger(__name__)


def timeit(method):
    """Execute a timer when a method starts.

    Used as annotation, allows to log the time a method or function takes to
    execute.
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        LOGGER.info('[time] method: %r (%r, %r) %2.2f sec' %
                    (method.__name__, args, kw, te-ts))
        return result
    return timed
