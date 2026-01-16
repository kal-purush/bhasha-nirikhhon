#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  outputUtils
#
#  Copyright 2014 - 2015 Claudio Giordano <claudio.giordano@autistici.org>
#
#  License GPLv3 https://www.gnu.org/licenses/gpl.html

def getConfirm(inputMessage = "Confirm action? [y/N]", validAnswer = "y"):
    choosed = False
    answer = False
    while (choosed == False):
        selection = raw_input(inputMessage + " ")
        if (selection == validAnswer):
            answer = True
        choosed = True

    return answer