"""
This module includes helping classes and methods for common logging tasks.
"""

import logging


class LoggerMixin(object):
    """ Gets a logger for the current class. Classes must inherit from this
        in order to use self.logger without the need of declaring it in every
        class. Reference:
        https://easyaspython.com/mixins-for-fun-and-profit-cb9962760556
    """
    @property
    def logger(self):
        """
            Gets a logger for the current class.

            Args: None.

            Returns:
                logger: logger for the current class.
        """
        name = '.'.join([
            self.__module__,
            self.__class__.__name__
        ])
        return logging.getLogger(name)
