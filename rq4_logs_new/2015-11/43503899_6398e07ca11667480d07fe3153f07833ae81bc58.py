#!/usr/bin/env python3

""" Graded Lab #4 for Inf1340, Fall 2015

    This program computes federal and provincial sales tax.
    Assume that the provincial sales tax is 5 percent and
    the federal sales tax is 2.5 percent. It displays the
    amount of the purchase, the provincial sales tax, the
    federal sales tax, the total tax, and the total of the
    sale.
"""

__author__ = 'Susan Sim'
__email__ = "ses@drsusansim.org"
__copyright__ = "2015 Susan Sim"
__license__ = "MIT License"

# Rewrite this code to use global constants, local variables and functions
# Output the text to a file instead of printing it


def bill_of_sale(purchase):

    print ("Amount of purchase: {0:.2f}".format(purchase))
    print ("Provincial tax: {0:.2f}".format(purchase * .05))
    print ("Federal tax: {0:.2f}".format(purchase * .025))
    print ("Total tax: {0:.2f}".format(purchase * .075))
    print ("Total sale: {0:.2f}".format(purchase * 1.075))