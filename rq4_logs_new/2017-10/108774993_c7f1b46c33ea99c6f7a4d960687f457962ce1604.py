#!/usr/bin/env python

from gimpfu import *

def python_pytest(img, layer) :
    # Actual plug-in code will go here
return

register(
       	"python-fu-pixel-art",
	"Does something",
	"Does something terribly useful",
	"Your name",
	"Your name",
	"2009",
	"Py Test...",
	"*",
	[
	],
	[],
	python_pytest,
        menu="/Filters/Distorts")

main()