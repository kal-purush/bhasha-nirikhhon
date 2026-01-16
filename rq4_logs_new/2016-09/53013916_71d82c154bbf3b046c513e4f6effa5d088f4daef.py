

# -*- coding: utf-8 -*-
################################################################################
# Copyright 2014, The Open Aggregator
#   GNU General Public License, Ver. 3 (see docs/license.txt)
################################################################################

"""This module illustrates how to write your docstring in OpenEst
and other projects related to OpenEst."""


__copyright__ = "Copyright 2014, The Open Aggregator"
__license__ = "GPL"

__author__ = "James Rising"
__credits__ = ["James Rising", "Solomon Hsiang", "Bob Kopp"]
__maintainer__ = "James Rising"
__email__ = "jar2234@columbia.edu"

__status__ = "Production"
__version__ = "$Revision$"

import numpy, scipy

class MainClass1(object):
    """This class docstring shows how to use sphinx and rst syntax

    The first line is brief explanation, which may be completed with 
    a longer one. For instance to discuss about its methods. The only
    method here is :func:`function1`'s. Identify class params, data types 
    and return values like this: 

    :param arg1: description
    :param arg2: description
    :type arg1: type description
    :type arg1: type description
    :return: return description
    :rtype: the return type description


    :Example:

    followed by a blank line

    .. seealso:: You can add references here, like the numpy documentation
    .. warnings also:: You can also provide warnings
    .. note:: something you want users to know
    .. todo:: and any features or other items for future implementation

    """

  def some_function(arg1, arg2, arg3):
    """
    returns (arg1 / arg2) + arg3

    :param arg1: the first value
    :param arg2: the first value
    :param arg3: the first value
    :type arg1: int, float,...
    :type arg2: int, float,...
    :type arg3: int, float,...
    :returns: arg1/arg2 +arg3
    :rtype: int, float

    
    :Example copied from terminal output:

    >>> import example
    >>> a = example.MainClass1()
    >>> a.some_function1(1,1,1)
    2

    .. note:: can be useful to emphasize
        important feature
    .. seealso:: :class:`MainClass2`
    .. warning:: arg2 must be non-zero.
    .. todo:: check that arg2 is non zero.
    """

    return (arg1/arg2) + arg3