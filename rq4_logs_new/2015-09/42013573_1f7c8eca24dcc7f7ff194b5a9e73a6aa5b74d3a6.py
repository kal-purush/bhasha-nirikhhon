#!/usr/bin/python

import re, os
"""
link ext [[url|text]] -> [text](url)
link int [[pagename|Link text]]
link int [[pagename]]
anchor internal link [[pagename#anchor|this Section]].

unordered list "  * <text>" -> "* <text>"
ordered list   "  - <text>" -> "<num>. <text>" or "#. <text>" (ext: fancy_lists)

footnote = "((footnote text))" -> "^[<inline footnote>]" (ext: inline_notes)

codeblock "<code><text></code>" -> "```\n<text>\n```", or "`<inline code>`"
codeblock languaged "<code <lang>><text></code>" -> "```<lang>\n<text>\n```"

italic "//<italictext>//" -> "*<emph>*" or "_<emph>_"
bold "**<boldtext>**" 	-> "**<strong>**" or "__<strong>__"
bold+italic "//**<boldtalic>**//" 	-> "***<strong>***" or "___<strong>___"
strikeout "<del><struck></del>" -> ~~<struck>~~" (ext: strikeout)
monospace "''<mono>''" -> "`<mono>`"

superscript "<super><text></super>" -> "^<text>^"
subscript "<sub><text></sub>" -> "~<text>~"

identical (or do not need changing) markers

blockquote (seems to be the same)
horizontal rule "-{4,}" - > "-{3,}" (so it's basically fine)

Dokuwiki:

^ Heading 1      ^ Heading 2       ^ Heading 3          ^
| Row 1 Col 1    | Row 1 Col 2     | Row 1 Col 3        |
| Row 2 Col 1    | some colspan (note the double pipe) ||
| Row 3 Col 1    | Row 3 Col 2     | Row 3 Col 3        |


Markdown (ext: pipe_tables):

heading 1		| heading 2			| heading 3
----------------|-------------------|-----------
row 1 col 1		| row 1 col 2		| row 1 col 3
row 2 col 1		| colspan??		| row 1 col 3
row 3 col 1		| row 3 col 2		| row 3 col 3

h1 "====== <text> ======" -> "# <text>"
h2 "===== <text> =====" -> "## <text>"
h3 "==== <text> ====" -> "### <text>"
h4 "=== <text> ===" -> "#### <text>"
h5 "== <text> ==" -> "##### <text>"


downloadable code blocks "<file <lang> <filename>> <sourcecode> </file>" -> ???
tag "{{tag><taglist>}} -> "%tag:<taglist>
"""

#print "egress"

conversionRules = \
[(r"^====== ?([\s\S]+) ?======$", r"# \1\n\n"),	#headings
(r"^===== ?([\s\S]+) ?=====$",r"## \1\n\n"),
(r"^==== ?([\s\S]+) ?====$",r"### \1\n\n"),
(r"^=== ?([\s\S]+) ?===$",r"#### \1\n\n"),
(r"^== ?([\s\S]+) ?==$",r"##### \1\n\n"),
(r"\(\(([\S\s]+)\)\)", r"\^\[\1\]"),				#footnotes
(r"<sub>([\s\S]+)</sub>",r"~\1~"),				#subscript
(r"<super>([\s\S]+)</super>",r"~\1~"),			#superscript
(r"<del>([\s\S]+)</del>",r"~~\1~~"),				#strikeout
(r"<code( [a-zA-Z]+)?>([\s\S]+)</code>",r"\n```\1\n\2\n```\n"),#codeblock with optional language attribute
(r"''([\S\s]+)''",r"`\1`"),						#monospace/inline code
(r"^  ([\S\s]+)$",r"\n    \1\n"),					#indented monospace/inline code
(r"//([\s\S]+)//",r"*\1*"),						#italic
(r"\[\[(http://[a-zA-Z-_./?&=0-9]+)\|([A-Za-z0-9 ]+)\]\]",r"[\2](\1)"),	#link
#("**([\s\S]+)*","**\1**"),						#bold is the same: **<bold>**
(r"{{tag>([a-zA-Z ,]+)}}",r"%tags:\1")]			#tag list


#print "Lol"

for cwd, dirs, files in os.walk('../pages'):
	for f in files:
		if f[-3:] == 'txt':
			print("converting "+f)
			#fyl = open(os.path.join(cwd,f), 'r')
			fyl = open("../pages/almondmilk.txt", 'r')
			#pageString = fyl.read()
			converted = open(os.path.join("../converted", f[:-3]+"md"), 'w')
			converted.close()#blank the extant file
			for line in fyl.readlines():
				for rule in conversionRules:
					#print rule
					pat = re.compile(rule[0])
					pageString = re.sub(pat, rule[1], line)
					#print pageString
				converted = open(os.path.join("../converted", f[:-3]+"md"), 'a')
				converted.write(pageString)
			converted.close()
			fyl.close()