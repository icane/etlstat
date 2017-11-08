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
logger = log.getLogger(__name__)


def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        logger.info('[time] method: %r (%r, %r) %2.2f sec' % (method.__name__, args, kw, te-ts))
        return result

    return timed
