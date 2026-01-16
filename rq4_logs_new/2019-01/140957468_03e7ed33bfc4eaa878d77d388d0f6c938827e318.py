#!/usr/bin/env fontforge
import fontforge
import sys, io

if len(sys.argv) < 2:
    print('Usage: ' + sys.argv[0] + ' font_file [character_files ...]')
    sys.exit()

font_file = sys.argv[1]
font = fontforge.open(font_file)
font_subset = fontforge.font()

#if font.is_cid:
#    font.cidFlatten()

# Add ASCII codepoints as default
codepoints = [i for i in range(128)]

character_files = sys.argv[2:]
for f in character_files:
    with io.open(f, encoding='utf8') as fp:
        characters = fp.read().strip().split('\n')
        for c in characters:
            codepoints.append(ord(c))

for codepoint in codepoints:
    encoding = font.findEncodingSlot(codepoint)
    if encoding != -1:
        font.selection.select(('singletons', 'unicode'), codepoint)
        font.copy()
        font_subset.createChar(codepoint)
        font_subset.selection.select(('singletons', 'unicode'), codepoint)
        font_subset.paste()
    else:
        print(unichr(codepoint) + ' not in ' + font_file)

subset_file = font_file.split('.')[0] + '_subset.' + font_file.split('.')[1]
font_subset.generate(subset_file)
print(subset_file + ' generated.')