import string
import json
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding='utf-8')

export_path = r'.\emojis.json'

# Copy and paste the webpage using
# https://www.unicode.org/emoji/charts/full-emoji-list.html
# ctrl+a ctrl+c ctrl+v into a text editor to get the string below.
unicode_website = """
[Unicode]Emoji Charts

Adopt a Character
AAC Animation
Full Emoji List, v12.1
For the new beta version, see v13.0β.
Index & Help | Images & Rights | Spec | Proposing Additions

This chart provides a list of the Unicode emoji characters and sequences, with images from different vendors, CLDR name, date, source, and keywords. The ordering of the emoji and the annotations are based on Unicode CLDR data. Emoji sequences have more than one code point in the Code column. Recently-added emoji are marked by a ⊛ in the name and outlined images; their images may show as a group with “…” before and after.

Emoji with skin-tones are not listed here: see Full Skin Tone List.

For counts of emoji, see Emoji Counts.

While these charts use a particular version of the Unicode Emoji data files, the images and format may be updated at any time. For any production usage, consult those data files. For information about the contents of each column, such as the CLDR Short Name, click on the column header. For further information, see Index & Help.

Smileys & Emotion
face-smiling
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1	U+1F600	😀	😀	😀	😀	😀	😀	😀	😀	😀	—	—	—	grinning face
2	U+1F603	😃	😃	😃	😃	😃	😃	😃	😃	😃	😃	😃	😃	grinning face with big eyes
3	U+1F604	😄	😄	😄	😄	😄	😄	😄	😄	😄	😄	—	—	grinning face with smiling eyes
4	U+1F601	😁	😁	😁	😁	😁	😁	😁	😁	😁	😁	😁	😁	beaming face with smiling eyes
5	U+1F606	😆	😆	😆	😆	😆	😆	😆	😆	😆	—	😆	—	grinning squinting face
6	U+1F605	😅	😅	😅	😅	😅	😅	😅	😅	😅	—	😅	—	grinning face with sweat
7	U+1F923	🤣	🤣	🤣	🤣	🤣	🤣	🤣	🤣	—	—	—	—	rolling on the floor laughing
8	U+1F602	😂	😂	😂	😂	😂	😂	😂	😂	😂	😂	—	😂	face with tears of joy
9	U+1F642	🙂	🙂	🙂	🙂	🙂	🙂	🙂	🙂	🙂	—	—	—	slightly smiling face
10	U+1F643	🙃	🙃	🙃	🙃	🙃	🙃	🙃	🙃	—	—	—	—	upside-down face
11	U+1F609	😉	😉	😉	😉	😉	😉	😉	😉	😉	😉	😉	😉	winking face
12	U+1F60A	😊	😊	😊	😊	😊	😊	😊	😊	😊	😊	—	😊	smiling face with smiling eyes
13	U+1F607	😇	😇	😇	😇	😇	😇	😇	😇	—	—	—	—	smiling face with halo
face-affection
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
14	U+1F970	🥰	🥰	🥰	🥰	🥰	🥰	🥰	🥰	—	—	—	—	smiling face with hearts
15	U+1F60D	😍	😍	😍	😍	😍	😍	😍	😍	😍	😍	😍	😍	smiling face with heart-eyes
16	U+1F929	🤩	🤩	🤩	🤩	🤩	🤩	🤩	🤩	—	—	—	—	star-struck
17	U+1F618	😘	😘	😘	😘	😘	😘	😘	😘	😘	😘	—	😘	face blowing a kiss
18	U+1F617	😗	😗	😗	😗	😗	😗	😗	😗	—	—	—	—	kissing face
19	U+263A	☺	☺	☺	☺	☺	☺	☺	☺	☺	☺	—	☺	smiling face
20	U+1F61A	😚	😚	😚	😚	😚	😚	😚	😚	😚	😚	—	😚	kissing face with closed eyes
21	U+1F619	😙	😙	😙	😙	😙	😙	😙	😙	—	—	—	—	kissing face with smiling eyes
face-tongue
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
22	U+1F60B	😋	😋	😋	😋	😋	😋	😋	😋	😋	—	😋	—	face savoring food
23	U+1F61B	😛	😛	😛	😛	😛	😛	😛	😛	—	—	—	—	face with tongue
24	U+1F61C	😜	😜	😜	😜	😜	😜	😜	😜	😜	😜	😜	😜	winking face with tongue
25	U+1F92A	🤪	🤪	🤪	🤪	🤪	🤪	🤪	🤪	—	—	—	—	zany face
26	U+1F61D	😝	😝	😝	😝	😝	😝	😝	😝	😝	😝	—	—	squinting face with tongue
27	U+1F911	🤑	🤑	🤑	🤑	🤑	🤑	🤑	🤑	—	—	—	—	money-mouth face
face-hand
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
28	U+1F917	🤗	🤗	🤗	🤗	🤗	🤗	🤗	🤗	—	—	—	—	hugging face
29	U+1F92D	🤭	🤭	🤭	🤭	🤭	🤭	🤭	🤭	—	—	—	—	face with hand over mouth
30	U+1F92B	🤫	🤫	🤫	🤫	🤫	🤫	🤫	🤫	—	—	—	—	shushing face
31	U+1F914	🤔	🤔	🤔	🤔	🤔	🤔	🤔	🤔	—	—	—	—	thinking face
face-neutral-skeptical
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
32	U+1F910	🤐	🤐	🤐	🤐	🤐	🤐	🤐	🤐	—	—	—	—	zipper-mouth face
33	U+1F928	🤨	🤨	🤨	🤨	🤨	🤨	🤨	🤨	—	—	—	—	face with raised eyebrow
34	U+1F610	😐	😐	😐	😐	😐	😐	😐	😐	—	—	—	—	neutral face
35	U+1F611	😑	😑	😑	😑	😑	😑	😑	😑	—	—	—	—	expressionless face
36	U+1F636	😶	😶	😶	😶	😶	😶	😶	😶	—	—	—	—	face without mouth
37	U+1F60F	😏	😏	😏	😏	😏	😏	😏	😏	😏	😏	😏	😏	smirking face
38	U+1F612	😒	😒	😒	😒	😒	😒	😒	😒	😒	😒	😒	😒	unamused face
39	U+1F644	🙄	🙄	🙄	🙄	🙄	🙄	🙄	🙄	—	—	—	—	face with rolling eyes
40	U+1F62C	😬	😬	😬	😬	😬	😬	😬	😬	—	—	—	—	grimacing face
41	U+1F925	🤥	🤥	🤥	🤥	🤥	🤥	🤥	🤥	—	—	—	—	lying face
face-sleepy
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
42	U+1F60C	😌	😌	😌	😌	😌	😌	😌	😌	😌	😌	😌	😌	relieved face
43	U+1F614	😔	😔	😔	😔	😔	😔	😔	😔	😔	😔	😔	😔	pensive face
44	U+1F62A	😪	😪	😪	😪	😪	😪	😪	😪	😪	😪	—	😪	sleepy face
45	U+1F924	🤤	🤤	🤤	🤤	🤤	🤤	🤤	🤤	—	—	—	—	drooling face
46	U+1F634	😴	😴	😴	😴	😴	😴	😴	😴	—	—	—	—	sleeping face
face-unwell
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
47	U+1F637	😷	😷	😷	😷	😷	😷	😷	😷	😷	😷	—	😷	face with medical mask
48	U+1F912	🤒	🤒	🤒	🤒	🤒	🤒	🤒	🤒	—	—	—	—	face with thermometer
49	U+1F915	🤕	🤕	🤕	🤕	🤕	🤕	🤕	🤕	—	—	—	—	face with head-bandage
50	U+1F922	🤢	🤢	🤢	🤢	🤢	🤢	🤢	🤢	—	—	—	—	nauseated face
51	U+1F92E	🤮	🤮	🤮	🤮	🤮	🤮	🤮	🤮	—	—	—	—	face vomiting
52	U+1F927	🤧	🤧	🤧	🤧	🤧	🤧	🤧	🤧	—	—	—	—	sneezing face
53	U+1F975	🥵	🥵	🥵	🥵	🥵	🥵	🥵	🥵	—	—	—	—	hot face
54	U+1F976	🥶	🥶	🥶	🥶	🥶	🥶	🥶	🥶	—	—	—	—	cold face
55	U+1F974	🥴	🥴	🥴	🥴	🥴	🥴	🥴	🥴	—	—	—	—	woozy face
56	U+1F635	😵	😵	😵	😵	😵	😵	😵	😵	😵	—	😵	😵	dizzy face
57	U+1F92F	🤯	🤯	🤯	🤯	🤯	🤯	🤯	🤯	—	—	—	—	exploding head
face-hat
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
58	U+1F920	🤠	🤠	🤠	🤠	🤠	🤠	🤠	🤠	—	—	—	—	cowboy hat face
59	U+1F973	🥳	🥳	🥳	🥳	🥳	🥳	🥳	🥳	—	—	—	—	partying face
face-glasses
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
60	U+1F60E	😎	😎	😎	😎	😎	😎	😎	😎	😎	—	—	—	smiling face with sunglasses
61	U+1F913	🤓	🤓	🤓	🤓	🤓	🤓	🤓	🤓	—	—	—	—	nerd face
62	U+1F9D0	🧐	🧐	🧐	🧐	🧐	🧐	🧐	🧐	—	—	—	—	face with monocle
face-concerned
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
63	U+1F615	😕	😕	😕	😕	😕	😕	😕	😕	😕	—	—	—	confused face
64	U+1F61F	😟	😟	😟	😟	😟	😟	😟	😟	😟	—	—	—	worried face
65	U+1F641	🙁	🙁	🙁	🙁	🙁	🙁	🙁	🙁	—	—	—	—	slightly frowning face
66	U+2639	☹	☹	☹	☹	☹	☹	☹	☹	—	—	—	—	frowning face
67	U+1F62E	😮	😮	😮	😮	😮	😮	😮	😮	—	—	—	—	face with open mouth
68	U+1F62F	😯	😯	😯	😯	😯	😯	😯	😯	—	—	—	—	hushed face
69	U+1F632	😲	😲	😲	😲	😲	😲	😲	😲	😲	😲	—	😲	astonished face
70	U+1F633	😳	😳	😳	😳	😳	😳	😳	😳	😳	😳	—	😳	flushed face
71	U+1F97A	🥺	🥺	🥺	🥺	🥺	🥺	🥺	🥺	—	—	—	—	pleading face
72	U+1F626	😦	😦	😦	😦	😦	😦	😦	😦	—	—	—	—	frowning face with open mouth
73	U+1F627	😧	😧	😧	😧	😧	😧	😧	😧	—	—	—	—	anguished face
74	U+1F628	😨	😨	😨	😨	😨	😨	😨	😨	😨	😨	—	😨	fearful face
75	U+1F630	😰	😰	😰	😰	😰	😰	😰	😰	😰	😰	—	😰	anxious face with sweat
76	U+1F625	😥	😥	😥	😥	😥	😥	😥	😥	😥	😥	—	—	sad but relieved face
77	U+1F622	😢	😢	😢	😢	😢	😢	😢	😢	😢	😢	😢	😢	crying face
78	U+1F62D	😭	😭	😭	😭	😭	😭	😭	😭	😭	😭	😭	😭	loudly crying face
79	U+1F631	😱	😱	😱	😱	😱	😱	😱	😱	😱	😱	😱	😱	face screaming in fear
80	U+1F616	😖	😖	😖	😖	😖	😖	😖	😖	😖	😖	😖	😖	confounded face
81	U+1F623	😣	😣	😣	😣	😣	😣	😣	😣	😣	😣	😣	😣	persevering face
82	U+1F61E	😞	😞	😞	😞	😞	😞	😞	😞	😞	😞	😞	—	disappointed face
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
83	U+1F613	😓	😓	😓	😓	😓	😓	😓	😓	😓	😓	😓	😓	downcast face with sweat
84	U+1F629	😩	😩	😩	😩	😩	😩	😩	😩	😩	—	—	😩	weary face
85	U+1F62B	😫	😫	😫	😫	😫	😫	😫	😫	😫	—	—	😫	tired face
86	U+1F971	🥱	🥱	🥱	🥱	🥱	🥱	🥱	🥱	—	—	—	—	yawning face
face-negative
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
87	U+1F624	😤	😤	😤	😤	😤	😤	😤	😤	😤	—	—	😤	face with steam from nose
88	U+1F621	😡	😡	😡	😡	😡	😡	😡	😡	😡	😡	😡	😡	pouting face
89	U+1F620	😠	😠	😠	😠	😠	😠	😠	😠	😠	😠	😠	😠	angry face
90	U+1F92C	🤬	🤬	🤬	🤬	🤬	🤬	🤬	🤬	—	—	—	—	face with symbols on mouth
91	U+1F608	😈	😈	😈	😈	😈	😈	😈	😈	—	—	—	—	smiling face with horns
92	U+1F47F	👿	👿	👿	👿	👿	👿	👿	👿	👿	👿	—	👿	angry face with horns
93	U+1F480	💀	💀	💀	💀	💀	💀	💀	💀	💀	💀	—	💀	skull
94	U+2620	☠	☠	☠	☠	☠	☠	☠	☠	—	—	—	—	skull and crossbones
face-costume
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
95	U+1F4A9	💩	💩	💩	💩	💩	💩	💩	💩	💩	💩	—	💩	pile of poo
96	U+1F921	🤡	🤡	🤡	🤡	🤡	🤡	🤡	🤡	—	—	—	—	clown face
97	U+1F479	👹	👹	👹	👹	👹	👹	👹	👹	👹	—	—	👹	ogre
98	U+1F47A	👺	👺	👺	👺	👺	👺	👺	👺	👺	—	—	👺	goblin
99	U+1F47B	👻	👻	👻	👻	👻	👻	👻	👻	👻	👻	—	👻	ghost
100	U+1F47D	👽	👽	👽	👽	👽	👽	👽	👽	👽	👽	—	👽	alien
101	U+1F47E	👾	👾	👾	👾	👾	👾	👾	👾	👾	👾	—	👾	alien monster
102	U+1F916	🤖	🤖	🤖	🤖	🤖	🤖	🤖	🤖	—	—	—	—	robot
cat-face
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
103	U+1F63A	😺	😺	😺	😺	😺	😺	😺	😺	😺	—	—	😺	grinning cat
104	U+1F638	😸	😸	😸	😸	😸	😸	😸	😸	😸	—	—	😸	grinning cat with smiling eyes
105	U+1F639	😹	😹	😹	😹	😹	😹	😹	😹	😹	—	—	😹	cat with tears of joy
106	U+1F63B	😻	😻	😻	😻	😻	😻	😻	😻	😻	—	—	😻	smiling cat with heart-eyes
107	U+1F63C	😼	😼	😼	😼	😼	😼	😼	😼	😼	—	—	😼	cat with wry smile
108	U+1F63D	😽	😽	😽	😽	😽	😽	😽	😽	😽	—	—	😽	kissing cat
109	U+1F640	🙀	🙀	🙀	🙀	🙀	🙀	🙀	🙀	🙀	—	—	🙀	weary cat
110	U+1F63F	😿	😿	😿	😿	😿	😿	😿	😿	😿	—	—	😿	crying cat
111	U+1F63E	😾	😾	😾	😾	😾	😾	😾	😾	😾	—	—	😾	pouting cat
monkey-face
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
112	U+1F648	🙈	🙈	🙈	🙈	🙈	🙈	🙈	🙈	🙈	—	—	🙈	see-no-evil monkey
113	U+1F649	🙉	🙉	🙉	🙉	🙉	🙉	🙉	🙉	🙉	—	—	🙉	hear-no-evil monkey
114	U+1F64A	🙊	🙊	🙊	🙊	🙊	🙊	🙊	🙊	🙊	—	—	🙊	speak-no-evil monkey
emotion
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
115	U+1F48B	💋	💋	💋	💋	💋	💋	💋	💋	💋	💋	💋	💋	kiss mark
116	U+1F48C	💌	💌	💌	💌	💌	💌	💌	💌	💌	—	💌	💌	love letter
117	U+1F498	💘	💘	💘	💘	💘	💘	💘	💘	💘	💘	—	💘	heart with arrow
118	U+1F49D	💝	💝	💝	💝	💝	💝	💝	💝	💝	💝	—	💝	heart with ribbon
119	U+1F496	💖	💖	💖	💖	💖	💖	💖	💖	💖	—	—	💖	sparkling heart
120	U+1F497	💗	💗	💗	💗	💗	💗	💗	💗	💗	💗	—	—	growing heart
121	U+1F493	💓	💓	💓	💓	💓	💓	💓	💓	💓	💓	💓	💓	beating heart
122	U+1F49E	💞	💞	💞	💞	💞	💞	💞	💞	💞	—	—	💞	revolving hearts
123	U+1F495	💕	💕	💕	💕	💕	💕	💕	💕	💕	—	💕	💕	two hearts
124	U+1F49F	💟	💟	💟	💟	💟	💟	💟	💟	💟	💟	—	—	heart decoration
125	U+2763	❣	❣	❣	❣	❣	❣	❣	❣	—	—	—	—	heart exclamation
126	U+1F494	💔	💔	💔	💔	💔	💔	💔	💔	💔	💔	💔	💔	broken heart
127	U+2764	❤	❤	❤	❤	❤	❤	❤	❤	❤	❤	❤	❤	red heart
128	U+1F9E1	🧡	🧡	🧡	🧡	🧡	🧡	🧡	🧡	—	—	—	—	orange heart
129	U+1F49B	💛	💛	💛	💛	💛	💛	💛	💛	💛	💛	—	💛	yellow heart
130	U+1F49A	💚	💚	💚	💚	💚	💚	💚	💚	💚	💚	—	💚	green heart
131	U+1F499	💙	💙	💙	💙	💙	💙	💙	💙	💙	💙	—	💙	blue heart
132	U+1F49C	💜	💜	💜	💜	💜	💜	💜	💜	💜	💜	—	💜	purple heart
133	U+1F90E	🤎	🤎	🤎	🤎	🤎	🤎	🤎	🤎	—	—	—	—	brown heart
134	U+1F5A4	🖤	🖤	🖤	🖤	🖤	🖤	🖤	🖤	—	—	—	—	black heart
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
135	U+1F90D	🤍	🤍	🤍	🤍	🤍	🤍	🤍	🤍	—	—	—	—	white heart
136	U+1F4AF	💯	💯	💯	💯	💯	💯	💯	💯	💯	—	—	💯	hundred points
137	U+1F4A2	💢	💢	💢	💢	💢	💢	💢	💢	💢	💢	💢	💢	anger symbol
138	U+1F4A5	💥	💥	💥	💥	💥	💥	💥	💥	💥	—	💥	💥	collision
139	U+1F4AB	💫	💫	💫	💫	💫	💫	💫	💫	💫	—	—	💫	dizzy
140	U+1F4A6	💦	💦	💦	💦	💦	💦	💦	💦	💦	💦	💦	💦	sweat droplets
141	U+1F4A8	💨	💨	💨	💨	💨	💨	💨	💨	💨	💨	💨	💨	dashing away
142	U+1F573	🕳	🕳	🕳	🕳	🕳	🕳	🕳	🕳	—	—	—	—	hole
143	U+1F4A3	💣	💣	💣	💣	💣	💣	💣	💣	💣	💣	💣	💣	bomb
144	U+1F4AC	💬	💬	💬	💬	💬	💬	💬	💬	💬	—	—	💬	speech balloon
145	U+1F441 U+FE0F U+200D U+1F5E8 U+FE0F	👁️‍🗨️	👁️‍🗨️	👁️‍🗨️	👁️‍🗨️	👁️‍🗨️	👁️‍🗨️	👁️‍🗨️	👁️‍🗨️	—	—	—	—	eye in speech bubble
146	U+1F5E8	🗨	🗨	🗨	🗨	🗨	🗨	🗨	🗨	—	—	—	—	left speech bubble
147	U+1F5EF	🗯	🗯	🗯	🗯	🗯	🗯	🗯	🗯	—	—	—	—	right anger bubble
148	U+1F4AD	💭	💭	💭	💭	💭	💭	💭	💭	—	—	—	—	thought balloon
149	U+1F4A4	💤	💤	💤	💤	💤	💤	💤	💤	💤	💤	💤	💤	zzz
People & Body
hand-fingers-open
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
150	U+1F44B	👋	👋	👋	👋	👋	👋	👋	👋	👋	👋	—	👋	waving hand
151	U+1F91A	🤚	🤚	🤚	🤚	🤚	🤚	🤚	🤚	—	—	—	—	raised back of hand
152	U+1F590	🖐	🖐	🖐	🖐	🖐	🖐	🖐	🖐	—	—	—	—	hand with fingers splayed
153	U+270B	✋	✋	✋	✋	✋	✋	✋	✋	✋	✋	✋	✋	raised hand
154	U+1F596	🖖	🖖	🖖	🖖	🖖	🖖	🖖	🖖	—	—	—	—	vulcan salute
hand-fingers-partial
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
155	U+1F44C	👌	👌	👌	👌	👌	👌	👌	👌	👌	👌	—	👌	OK hand
156	U+1F90F	🤏	🤏	🤏	🤏	🤏	🤏	🤏	🤏	—	—	—	—	pinching hand
157	U+270C	✌	✌	✌	✌	✌	✌	✌	✌	✌	✌	✌	✌	victory hand
158	U+1F91E	🤞	🤞	🤞	🤞	🤞	🤞	🤞	🤞	—	—	—	—	crossed fingers
159	U+1F91F	🤟	🤟	🤟	🤟	🤟	🤟	🤟	🤟	—	—	—	—	love-you gesture
160	U+1F918	🤘	🤘	🤘	🤘	🤘	🤘	🤘	🤘	—	—	—	—	sign of the horns
161	U+1F919	🤙	🤙	🤙	🤙	🤙	🤙	🤙	🤙	—	—	—	—	call me hand
hand-single-finger
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
162	U+1F448	👈	👈	👈	👈	👈	👈	👈	👈	👈	👈	—	👈	backhand index pointing left
163	U+1F449	👉	👉	👉	👉	👉	👉	👉	👉	👉	👉	—	👉	backhand index pointing right
164	U+1F446	👆	👆	👆	👆	👆	👆	👆	👆	👆	👆	—	👆	backhand index pointing up
165	U+1F595	🖕	🖕	🖕	🖕	🖕	🖕	🖕	🖕	—	—	—	—	middle finger
166	U+1F447	👇	👇	👇	👇	👇	👇	👇	👇	👇	👇	—	👇	backhand index pointing down
167	U+261D	☝	☝	☝	☝	☝	☝	☝	☝	☝	☝	—	☝	index pointing up
hand-fingers-closed
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
168	U+1F44D	👍	👍	👍	👍	👍	👍	👍	👍	👍	👍	👍	👍	thumbs up
169	U+1F44E	👎	👎	👎	👎	👎	👎	👎	👎	👎	👎	—	👎	thumbs down
170	U+270A	✊	✊	✊	✊	✊	✊	✊	✊	✊	✊	✊	✊	raised fist
171	U+1F44A	👊	👊	👊	👊	👊	👊	👊	👊	👊	👊	👊	👊	oncoming fist
172	U+1F91B	🤛	🤛	🤛	🤛	🤛	🤛	🤛	🤛	—	—	—	—	left-facing fist
173	U+1F91C	🤜	🤜	🤜	🤜	🤜	🤜	🤜	🤜	—	—	—	—	right-facing fist
hands
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
174	U+1F44F	👏	👏	👏	👏	👏	👏	👏	👏	👏	👏	—	👏	clapping hands
175	U+1F64C	🙌	🙌	🙌	🙌	🙌	🙌	🙌	🙌	🙌	🙌	—	🙌	raising hands
176	U+1F450	👐	👐	👐	👐	👐	👐	👐	👐	👐	👐	—	—	open hands
177	U+1F932	🤲	🤲	🤲	🤲	🤲	🤲	🤲	🤲	—	—	—	—	palms up together
178	U+1F91D	🤝	🤝	🤝	🤝	🤝	🤝	🤝	🤝	—	—	—	—	handshake
179	U+1F64F	🙏	🙏	🙏	🙏	🙏	🙏	🙏	🙏	🙏	🙏	—	🙏	folded hands
hand-prop
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
180	U+270D	✍	✍	✍	✍	✍	✍	✍	✍	—	—	—	—	writing hand
181	U+1F485	💅	💅	💅	💅	💅	💅	💅	💅	💅	💅	—	💅	nail polish
182	U+1F933	🤳	🤳	🤳	🤳	🤳	🤳	🤳	🤳	—	—	—	—	selfie
body-parts
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
183	U+1F4AA	💪	💪	💪	💪	💪	💪	💪	💪	💪	💪	—	💪	flexed biceps
184	U+1F9BE	🦾	🦾	🦾	🦾	🦾	🦾	🦾	🦾	—	—	—	—	mechanical arm
185	U+1F9BF	🦿	🦿	🦿	🦿	🦿	🦿	🦿	🦿	—	—	—	—	mechanical leg
186	U+1F9B5	🦵	🦵	🦵	🦵	🦵	🦵	🦵	🦵	—	—	—	—	leg
187	U+1F9B6	🦶	🦶	🦶	🦶	🦶	🦶	🦶	🦶	—	—	—	—	foot
188	U+1F442	👂	👂	👂	👂	👂	👂	👂	👂	👂	👂	👂	👂	ear
189	U+1F9BB	🦻	🦻	🦻	🦻	🦻	🦻	🦻	🦻	—	—	—	—	ear with hearing aid
190	U+1F443	👃	👃	👃	👃	👃	👃	👃	👃	👃	👃	—	👃	nose
191	U+1F9E0	🧠	🧠	🧠	🧠	🧠	🧠	🧠	🧠	—	—	—	—	brain
192	U+1F9B7	🦷	🦷	🦷	🦷	🦷	🦷	🦷	🦷	—	—	—	—	tooth
193	U+1F9B4	🦴	🦴	🦴	🦴	🦴	🦴	🦴	🦴	—	—	—	—	bone
194	U+1F440	👀	👀	👀	👀	👀	👀	👀	👀	👀	👀	👀	👀	eyes
195	U+1F441	👁	👁	👁	👁	👁	👁	👁	👁	—	—	—	—	eye
196	U+1F445	👅	👅	👅	👅	👅	👅	👅	👅	👅	—	—	👅	tongue
197	U+1F444	👄	👄	👄	👄	👄	👄	👄	👄	👄	👄	—	👄	mouth
person
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
198	U+1F476	👶	👶	👶	👶	👶	👶	👶	👶	👶	👶	—	👶	baby
199	U+1F9D2	🧒	🧒	🧒	🧒	🧒	🧒	🧒	🧒	—	—	—	—	child
200	U+1F466	👦	👦	👦	👦	👦	👦	👦	👦	👦	👦	—	—	boy
201	U+1F467	👧	👧	👧	👧	👧	👧	👧	👧	👧	👧	—	—	girl
202	U+1F9D1	🧑	🧑	🧑	🧑	🧑	🧑	🧑	🧑	—	—	—	—	person
203	U+1F471	👱	👱	👱	👱	👱	👱	👱	👱	👱	👱	—	👱	person: blond hair
204	U+1F468	👨	👨	👨	👨	👨	👨	👨	👨	👨	👨	—	👨	man
205	U+1F9D4	🧔	🧔	🧔	🧔	🧔	🧔	🧔	🧔	—	—	—	—	man: beard
206	U+1F468 U+200D U+1F9B0	👨‍🦰	👨‍🦰	👨‍🦰	👨‍🦰	👨‍🦰	👨‍🦰	👨‍🦰	👨‍🦰	—	—	—	—	man: red hair
207	U+1F468 U+200D U+1F9B1	👨‍🦱	👨‍🦱	👨‍🦱	👨‍🦱	👨‍🦱	👨‍🦱	👨‍🦱	👨‍🦱	—	—	—	—	man: curly hair
208	U+1F468 U+200D U+1F9B3	👨‍🦳	👨‍🦳	👨‍🦳	👨‍🦳	👨‍🦳	👨‍🦳	👨‍🦳	👨‍🦳	—	—	—	—	man: white hair
209	U+1F468 U+200D U+1F9B2	👨‍🦲	👨‍🦲	👨‍🦲	👨‍🦲	👨‍🦲	👨‍🦲	👨‍🦲	👨‍🦲	—	—	—	—	man: bald
210	U+1F469	👩	👩	👩	👩	👩	👩	👩	👩	👩	👩	—	👩	woman
211	U+1F469 U+200D U+1F9B0	👩‍🦰	👩‍🦰	👩‍🦰	👩‍🦰	👩‍🦰	👩‍🦰	👩‍🦰	👩‍🦰	—	—	—	—	woman: red hair
212	U+1F9D1 U+200D U+1F9B0	🧑‍🦰	🧑‍🦰	🧑‍🦰	—	—	🧑‍🦰	—	—	—	—	—	—	⊛ person: red hair
213	U+1F469 U+200D U+1F9B1	👩‍🦱	👩‍🦱	👩‍🦱	👩‍🦱	👩‍🦱	👩‍🦱	👩‍🦱	👩‍🦱	—	—	—	—	woman: curly hair
214	U+1F9D1 U+200D U+1F9B1	🧑‍🦱	🧑‍🦱	🧑‍🦱	—	—	🧑‍🦱	—	—	—	—	—	—	⊛ person: curly hair
215	U+1F469 U+200D U+1F9B3	👩‍🦳	👩‍🦳	👩‍🦳	👩‍🦳	👩‍🦳	👩‍🦳	👩‍🦳	👩‍🦳	—	—	—	—	woman: white hair
216	U+1F9D1 U+200D U+1F9B3	🧑‍🦳	🧑‍🦳	🧑‍🦳	—	—	🧑‍🦳	—	—	—	—	—	—	⊛ person: white hair
217	U+1F469 U+200D U+1F9B2	👩‍🦲	👩‍🦲	👩‍🦲	👩‍🦲	👩‍🦲	👩‍🦲	👩‍🦲	👩‍🦲	—	—	—	—	woman: bald
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
218	U+1F9D1 U+200D U+1F9B2	🧑‍🦲	🧑‍🦲	🧑‍🦲	—	—	🧑‍🦲	—	—	—	—	—	—	⊛ person: bald
219	U+1F471 U+200D U+2640 U+FE0F	👱‍♀️	👱‍♀️	👱‍♀️	👱‍♀️	👱‍♀️	👱‍♀️	👱‍♀️	👱‍♀️	—	—	—	—	woman: blond hair
220	U+1F471 U+200D U+2642 U+FE0F	👱‍♂️	👱‍♂️	👱‍♂️	👱‍♂️	👱‍♂️	👱‍♂️	👱‍♂️	👱‍♂️	—	—	—	—	man: blond hair
221	U+1F9D3	🧓	🧓	🧓	🧓	🧓	🧓	🧓	🧓	—	—	—	—	older person
222	U+1F474	👴	👴	👴	👴	👴	👴	👴	👴	👴	👴	—	👴	old man
223	U+1F475	👵	👵	👵	👵	👵	👵	👵	👵	👵	👵	—	👵	old woman
person-gesture
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
224	U+1F64D	🙍	🙍	🙍	🙍	🙍	🙍	🙍	🙍	🙍	—	—	🙍	person frowning
225	U+1F64D U+200D U+2642 U+FE0F	🙍‍♂️	🙍‍♂️	🙍‍♂️	🙍‍♂️	🙍‍♂️	🙍‍♂️	🙍‍♂️	🙍‍♂️	—	—	—	—	man frowning
226	U+1F64D U+200D U+2640 U+FE0F	🙍‍♀️	🙍‍♀️	🙍‍♀️	🙍‍♀️	🙍‍♀️	🙍‍♀️	🙍‍♀️	🙍‍♀️	—	—	—	—	woman frowning
227	U+1F64E	🙎	🙎	🙎	🙎	🙎	🙎	🙎	🙎	🙎	—	—	🙎	person pouting
228	U+1F64E U+200D U+2642 U+FE0F	🙎‍♂️	🙎‍♂️	🙎‍♂️	🙎‍♂️	🙎‍♂️	🙎‍♂️	🙎‍♂️	🙎‍♂️	—	—	—	—	man pouting
229	U+1F64E U+200D U+2640 U+FE0F	🙎‍♀️	🙎‍♀️	🙎‍♀️	🙎‍♀️	🙎‍♀️	🙎‍♀️	🙎‍♀️	🙎‍♀️	—	—	—	—	woman pouting
230	U+1F645	🙅	🙅	🙅	🙅	🙅	🙅	🙅	🙅	🙅	🙅	—	🙅	person gesturing NO
231	U+1F645 U+200D U+2642 U+FE0F	🙅‍♂️	🙅‍♂️	🙅‍♂️	🙅‍♂️	🙅‍♂️	🙅‍♂️	🙅‍♂️	🙅‍♂️	—	—	—	—	man gesturing NO
232	U+1F645 U+200D U+2640 U+FE0F	🙅‍♀️	🙅‍♀️	🙅‍♀️	🙅‍♀️	🙅‍♀️	🙅‍♀️	🙅‍♀️	🙅‍♀️	—	—	—	—	woman gesturing NO
233	U+1F646	🙆	🙆	🙆	🙆	🙆	🙆	🙆	🙆	🙆	🙆	—	🙆	person gesturing OK
234	U+1F646 U+200D U+2642 U+FE0F	🙆‍♂️	🙆‍♂️	🙆‍♂️	🙆‍♂️	🙆‍♂️	🙆‍♂️	🙆‍♂️	🙆‍♂️	—	—	—	—	man gesturing OK
235	U+1F646 U+200D U+2640 U+FE0F	🙆‍♀️	🙆‍♀️	🙆‍♀️	🙆‍♀️	🙆‍♀️	🙆‍♀️	🙆‍♀️	🙆‍♀️	—	—	—	—	woman gesturing OK
236	U+1F481	💁	💁	💁	💁	💁	💁	💁	💁	💁	💁	—	—	person tipping hand
237	U+1F481 U+200D U+2642 U+FE0F	💁‍♂️	💁‍♂️	💁‍♂️	💁‍♂️	💁‍♂️	💁‍♂️	💁‍♂️	💁‍♂️	—	—	—	—	man tipping hand
238	U+1F481 U+200D U+2640 U+FE0F	💁‍♀️	💁‍♀️	💁‍♀️	💁‍♀️	💁‍♀️	💁‍♀️	💁‍♀️	💁‍♀️	—	—	—	—	woman tipping hand
239	U+1F64B	🙋	🙋	🙋	🙋	🙋	🙋	🙋	🙋	🙋	—	—	🙋	person raising hand
240	U+1F64B U+200D U+2642 U+FE0F	🙋‍♂️	🙋‍♂️	🙋‍♂️	🙋‍♂️	🙋‍♂️	🙋‍♂️	🙋‍♂️	🙋‍♂️	—	—	—	—	man raising hand
241	U+1F64B U+200D U+2640 U+FE0F	🙋‍♀️	🙋‍♀️	🙋‍♀️	🙋‍♀️	🙋‍♀️	🙋‍♀️	🙋‍♀️	🙋‍♀️	—	—	—	—	woman raising hand
242	U+1F9CF	🧏	🧏	🧏	🧏	🧏	🧏	🧏	🧏	—	—	—	—	deaf person
243	U+1F9CF U+200D U+2642 U+FE0F	🧏‍♂️	🧏‍♂️	🧏‍♂️	🧏‍♂️	—	🧏‍♂️	🧏‍♂️	🧏‍♂️	—	—	—	—	deaf man
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
244	U+1F9CF U+200D U+2640 U+FE0F	🧏‍♀️	🧏‍♀️	🧏‍♀️	🧏‍♀️	—	🧏‍♀️	🧏‍♀️	🧏‍♀️	—	—	—	—	deaf woman
245	U+1F647	🙇	🙇	🙇	🙇	🙇	🙇	🙇	🙇	🙇	🙇	—	🙇	person bowing
246	U+1F647 U+200D U+2642 U+FE0F	🙇‍♂️	🙇‍♂️	🙇‍♂️	🙇‍♂️	🙇‍♂️	🙇‍♂️	🙇‍♂️	🙇‍♂️	—	—	—	—	man bowing
247	U+1F647 U+200D U+2640 U+FE0F	🙇‍♀️	🙇‍♀️	🙇‍♀️	🙇‍♀️	🙇‍♀️	🙇‍♀️	🙇‍♀️	🙇‍♀️	—	—	—	—	woman bowing
248	U+1F926	🤦	🤦	🤦	🤦	🤦	🤦	🤦	🤦	—	—	—	—	person facepalming
249	U+1F926 U+200D U+2642 U+FE0F	🤦‍♂️	🤦‍♂️	🤦‍♂️	🤦‍♂️	🤦‍♂️	🤦‍♂️	🤦‍♂️	🤦‍♂️	—	—	—	—	man facepalming
250	U+1F926 U+200D U+2640 U+FE0F	🤦‍♀️	🤦‍♀️	🤦‍♀️	🤦‍♀️	🤦‍♀️	🤦‍♀️	🤦‍♀️	🤦‍♀️	—	—	—	—	woman facepalming
251	U+1F937	🤷	🤷	🤷	🤷	🤷	🤷	🤷	🤷	—	—	—	—	person shrugging
252	U+1F937 U+200D U+2642 U+FE0F	🤷‍♂️	🤷‍♂️	🤷‍♂️	🤷‍♂️	🤷‍♂️	🤷‍♂️	🤷‍♂️	🤷‍♂️	—	—	—	—	man shrugging
253	U+1F937 U+200D U+2640 U+FE0F	🤷‍♀️	🤷‍♀️	🤷‍♀️	🤷‍♀️	🤷‍♀️	🤷‍♀️	🤷‍♀️	🤷‍♀️	—	—	—	—	woman shrugging
person-role
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
254	U+1F9D1 U+200D U+2695 U+FE0F	🧑‍⚕️	🧑‍⚕️	🧑‍⚕️	—	—	🧑‍⚕️	—	—	—	—	—	—	⊛ health worker
255	U+1F468 U+200D U+2695 U+FE0F	👨‍⚕️	👨‍⚕️	👨‍⚕️	👨‍⚕️	👨‍⚕️	👨‍⚕️	👨‍⚕️	👨‍⚕️	—	—	—	—	man health worker
256	U+1F469 U+200D U+2695 U+FE0F	👩‍⚕️	👩‍⚕️	👩‍⚕️	👩‍⚕️	👩‍⚕️	👩‍⚕️	👩‍⚕️	👩‍⚕️	—	—	—	—	woman health worker
257	U+1F9D1 U+200D U+1F393	🧑‍🎓	🧑‍🎓	🧑‍🎓	—	—	🧑‍🎓	—	—	—	—	—	—	⊛ student
258	U+1F468 U+200D U+1F393	👨‍🎓	👨‍🎓	👨‍🎓	👨‍🎓	👨‍🎓	👨‍🎓	👨‍🎓	👨‍🎓	—	—	—	—	man student
259	U+1F469 U+200D U+1F393	👩‍🎓	👩‍🎓	👩‍🎓	👩‍🎓	👩‍🎓	👩‍🎓	👩‍🎓	👩‍🎓	—	—	—	—	woman student
260	U+1F9D1 U+200D U+1F3EB	🧑‍🏫	🧑‍🏫	🧑‍🏫	—	—	🧑‍🏫	—	—	—	—	—	—	⊛ teacher
261	U+1F468 U+200D U+1F3EB	👨‍🏫	👨‍🏫	👨‍🏫	👨‍🏫	👨‍🏫	👨‍🏫	👨‍🏫	👨‍🏫	—	—	—	—	man teacher
262	U+1F469 U+200D U+1F3EB	👩‍🏫	👩‍🏫	👩‍🏫	👩‍🏫	👩‍🏫	👩‍🏫	👩‍🏫	👩‍🏫	—	—	—	—	woman teacher
263	U+1F9D1 U+200D U+2696 U+FE0F	🧑‍⚖️	🧑‍⚖️	🧑‍⚖️	—	—	🧑‍⚖️	—	—	—	—	—	—	⊛ judge
264	U+1F468 U+200D U+2696 U+FE0F	👨‍⚖️	👨‍⚖️	👨‍⚖️	👨‍⚖️	👨‍⚖️	👨‍⚖️	👨‍⚖️	👨‍⚖️	—	—	—	—	man judge
265	U+1F469 U+200D U+2696 U+FE0F	👩‍⚖️	👩‍⚖️	👩‍⚖️	👩‍⚖️	👩‍⚖️	👩‍⚖️	👩‍⚖️	👩‍⚖️	—	—	—	—	woman judge
266	U+1F9D1 U+200D U+1F33E	🧑‍🌾	🧑‍🌾	🧑‍🌾	—	—	🧑‍🌾	—	—	—	—	—	—	⊛ farmer
267	U+1F468 U+200D U+1F33E	👨‍🌾	👨‍🌾	👨‍🌾	👨‍🌾	👨‍🌾	👨‍🌾	👨‍🌾	👨‍🌾	—	—	—	—	man farmer
268	U+1F469 U+200D U+1F33E	👩‍🌾	👩‍🌾	👩‍🌾	👩‍🌾	👩‍🌾	👩‍🌾	👩‍🌾	👩‍🌾	—	—	—	—	woman farmer
269	U+1F9D1 U+200D U+1F373	🧑‍🍳	🧑‍🍳	🧑‍🍳	—	—	🧑‍🍳	—	—	—	—	—	—	⊛ cook
270	U+1F468 U+200D U+1F373	👨‍🍳	👨‍🍳	👨‍🍳	👨‍🍳	👨‍🍳	👨‍🍳	👨‍🍳	👨‍🍳	—	—	—	—	man cook
271	U+1F469 U+200D U+1F373	👩‍🍳	👩‍🍳	👩‍🍳	👩‍🍳	👩‍🍳	👩‍🍳	👩‍🍳	👩‍🍳	—	—	—	—	woman cook
272	U+1F9D1 U+200D U+1F527	🧑‍🔧	🧑‍🔧	🧑‍🔧	—	—	🧑‍🔧	—	—	—	—	—	—	⊛ mechanic
273	U+1F468 U+200D U+1F527	👨‍🔧	👨‍🔧	👨‍🔧	👨‍🔧	👨‍🔧	👨‍🔧	👨‍🔧	👨‍🔧	—	—	—	—	man mechanic
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
274	U+1F469 U+200D U+1F527	👩‍🔧	👩‍🔧	👩‍🔧	👩‍🔧	👩‍🔧	👩‍🔧	👩‍🔧	👩‍🔧	—	—	—	—	woman mechanic
275	U+1F9D1 U+200D U+1F3ED	🧑‍🏭	🧑‍🏭	🧑‍🏭	—	—	🧑‍🏭	—	—	—	—	—	—	⊛ factory worker
276	U+1F468 U+200D U+1F3ED	👨‍🏭	👨‍🏭	👨‍🏭	👨‍🏭	👨‍🏭	👨‍🏭	👨‍🏭	👨‍🏭	—	—	—	—	man factory worker
277	U+1F469 U+200D U+1F3ED	👩‍🏭	👩‍🏭	👩‍🏭	👩‍🏭	👩‍🏭	👩‍🏭	👩‍🏭	👩‍🏭	—	—	—	—	woman factory worker
278	U+1F9D1 U+200D U+1F4BC	🧑‍💼	🧑‍💼	🧑‍💼	—	—	🧑‍💼	—	—	—	—	—	—	⊛ office worker
279	U+1F468 U+200D U+1F4BC	👨‍💼	👨‍💼	👨‍💼	👨‍💼	👨‍💼	👨‍💼	👨‍💼	👨‍💼	—	—	—	—	man office worker
280	U+1F469 U+200D U+1F4BC	👩‍💼	👩‍💼	👩‍💼	👩‍💼	👩‍💼	👩‍💼	👩‍💼	👩‍💼	—	—	—	—	woman office worker
281	U+1F9D1 U+200D U+1F52C	🧑‍🔬	🧑‍🔬	🧑‍🔬	—	—	🧑‍🔬	—	—	—	—	—	—	⊛ scientist
282	U+1F468 U+200D U+1F52C	👨‍🔬	👨‍🔬	👨‍🔬	👨‍🔬	👨‍🔬	👨‍🔬	👨‍🔬	👨‍🔬	—	—	—	—	man scientist
283	U+1F469 U+200D U+1F52C	👩‍🔬	👩‍🔬	👩‍🔬	👩‍🔬	👩‍🔬	👩‍🔬	👩‍🔬	👩‍🔬	—	—	—	—	woman scientist
284	U+1F9D1 U+200D U+1F4BB	🧑‍💻	🧑‍💻	🧑‍💻	—	—	🧑‍💻	—	—	—	—	—	—	⊛ technologist
285	U+1F468 U+200D U+1F4BB	👨‍💻	👨‍💻	👨‍💻	👨‍💻	👨‍💻	👨‍💻	👨‍💻	👨‍💻	—	—	—	—	man technologist
286	U+1F469 U+200D U+1F4BB	👩‍💻	👩‍💻	👩‍💻	👩‍💻	👩‍💻	👩‍💻	👩‍💻	👩‍💻	—	—	—	—	woman technologist
287	U+1F9D1 U+200D U+1F3A4	🧑‍🎤	🧑‍🎤	🧑‍🎤	—	—	🧑‍🎤	—	—	—	—	—	—	⊛ singer
288	U+1F468 U+200D U+1F3A4	👨‍🎤	👨‍🎤	👨‍🎤	👨‍🎤	👨‍🎤	👨‍🎤	👨‍🎤	👨‍🎤	—	—	—	—	man singer
289	U+1F469 U+200D U+1F3A4	👩‍🎤	👩‍🎤	👩‍🎤	👩‍🎤	👩‍🎤	👩‍🎤	👩‍🎤	👩‍🎤	—	—	—	—	woman singer
290	U+1F9D1 U+200D U+1F3A8	🧑‍🎨	🧑‍🎨	🧑‍🎨	—	—	🧑‍🎨	—	—	—	—	—	—	⊛ artist
291	U+1F468 U+200D U+1F3A8	👨‍🎨	👨‍🎨	👨‍🎨	👨‍🎨	👨‍🎨	👨‍🎨	👨‍🎨	👨‍🎨	—	—	—	—	man artist
292	U+1F469 U+200D U+1F3A8	👩‍🎨	👩‍🎨	👩‍🎨	👩‍🎨	👩‍🎨	👩‍🎨	👩‍🎨	👩‍🎨	—	—	—	—	woman artist
293	U+1F9D1 U+200D U+2708 U+FE0F	🧑‍✈️	🧑‍✈️	🧑‍✈️	—	—	🧑‍✈️	—	—	—	—	—	—	⊛ pilot
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
294	U+1F468 U+200D U+2708 U+FE0F	👨‍✈️	👨‍✈️	👨‍✈️	👨‍✈️	👨‍✈️	👨‍✈️	👨‍✈️	👨‍✈️	—	—	—	—	man pilot
295	U+1F469 U+200D U+2708 U+FE0F	👩‍✈️	👩‍✈️	👩‍✈️	👩‍✈️	👩‍✈️	👩‍✈️	👩‍✈️	👩‍✈️	—	—	—	—	woman pilot
296	U+1F9D1 U+200D U+1F680	🧑‍🚀	🧑‍🚀	🧑‍🚀	—	—	🧑‍🚀	—	—	—	—	—	—	⊛ astronaut
297	U+1F468 U+200D U+1F680	👨‍🚀	👨‍🚀	👨‍🚀	👨‍🚀	👨‍🚀	👨‍🚀	👨‍🚀	👨‍🚀	—	—	—	—	man astronaut
298	U+1F469 U+200D U+1F680	👩‍🚀	👩‍🚀	👩‍🚀	👩‍🚀	👩‍🚀	👩‍🚀	👩‍🚀	👩‍🚀	—	—	—	—	woman astronaut
299	U+1F9D1 U+200D U+1F692	🧑‍🚒	🧑‍🚒	🧑‍🚒	—	—	🧑‍🚒	—	—	—	—	—	—	⊛ firefighter
300	U+1F468 U+200D U+1F692	👨‍🚒	👨‍🚒	👨‍🚒	👨‍🚒	👨‍🚒	👨‍🚒	👨‍🚒	👨‍🚒	—	—	—	—	man firefighter
301	U+1F469 U+200D U+1F692	👩‍🚒	👩‍🚒	👩‍🚒	👩‍🚒	👩‍🚒	👩‍🚒	👩‍🚒	👩‍🚒	—	—	—	—	woman firefighter
302	U+1F46E	👮	👮	👮	👮	👮	👮	👮	👮	👮	👮	—	👮	police officer
303	U+1F46E U+200D U+2642 U+FE0F	👮‍♂️	👮‍♂️	👮‍♂️	👮‍♂️	👮‍♂️	👮‍♂️	👮‍♂️	👮‍♂️	—	—	—	—	man police officer
304	U+1F46E U+200D U+2640 U+FE0F	👮‍♀️	👮‍♀️	👮‍♀️	👮‍♀️	👮‍♀️	👮‍♀️	👮‍♀️	👮‍♀️	—	—	—	—	woman police officer
305	U+1F575	🕵	🕵	🕵	🕵	🕵	🕵	🕵	🕵	—	—	—	—	detective
306	U+1F575 U+FE0F U+200D U+2642 U+FE0F	🕵️‍♂️	🕵️‍♂️	🕵️‍♂️	🕵️‍♂️	🕵️‍♂️	🕵️‍♂️	🕵️‍♂️	🕵️‍♂️	—	—	—	—	man detective
307	U+1F575 U+FE0F U+200D U+2640 U+FE0F	🕵️‍♀️	🕵️‍♀️	🕵️‍♀️	🕵️‍♀️	🕵️‍♀️	🕵️‍♀️	🕵️‍♀️	🕵️‍♀️	—	—	—	—	woman detective
308	U+1F482	💂	💂	💂	💂	💂	💂	💂	💂	💂	💂	—	—	guard
309	U+1F482 U+200D U+2642 U+FE0F	💂‍♂️	💂‍♂️	💂‍♂️	💂‍♂️	💂‍♂️	💂‍♂️	💂‍♂️	💂‍♂️	—	—	—	—	man guard
310	U+1F482 U+200D U+2640 U+FE0F	💂‍♀️	💂‍♀️	💂‍♀️	💂‍♀️	💂‍♀️	💂‍♀️	💂‍♀️	💂‍♀️	—	—	—	—	woman guard
311	U+1F477	👷	👷	👷	👷	👷	👷	👷	👷	👷	👷	—	👷	construction worker
312	U+1F477 U+200D U+2642 U+FE0F	👷‍♂️	👷‍♂️	👷‍♂️	👷‍♂️	👷‍♂️	👷‍♂️	👷‍♂️	👷‍♂️	—	—	—	—	man construction worker
313	U+1F477 U+200D U+2640 U+FE0F	👷‍♀️	👷‍♀️	👷‍♀️	👷‍♀️	👷‍♀️	👷‍♀️	👷‍♀️	👷‍♀️	—	—	—	—	woman construction worker
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
314	U+1F934	🤴	🤴	🤴	🤴	🤴	🤴	🤴	🤴	—	—	—	—	prince
315	U+1F478	👸	👸	👸	👸	👸	👸	👸	👸	👸	👸	—	👸	princess
316	U+1F473	👳	👳	👳	👳	👳	👳	👳	👳	👳	👳	—	👳	person wearing turban
317	U+1F473 U+200D U+2642 U+FE0F	👳‍♂️	👳‍♂️	👳‍♂️	👳‍♂️	👳‍♂️	👳‍♂️	👳‍♂️	👳‍♂️	—	—	—	—	man wearing turban
318	U+1F473 U+200D U+2640 U+FE0F	👳‍♀️	👳‍♀️	👳‍♀️	👳‍♀️	👳‍♀️	👳‍♀️	👳‍♀️	👳‍♀️	—	—	—	—	woman wearing turban
319	U+1F472	👲	👲	👲	👲	👲	👲	👲	👲	👲	👲	—	👲	person with skullcap
320	U+1F9D5	🧕	🧕	🧕	🧕	🧕	🧕	🧕	🧕	—	—	—	—	woman with headscarf
321	U+1F935	🤵	🤵	🤵	🤵	🤵	🤵	🤵	🤵	—	—	—	—	person in tuxedo
322	U+1F470	👰	👰	👰	👰	👰	👰	👰	👰	👰	—	—	👰	person with veil
323	U+1F930	🤰	🤰	🤰	🤰	🤰	🤰	🤰	🤰	—	—	—	—	pregnant woman
324	U+1F931	🤱	🤱	🤱	🤱	🤱	🤱	🤱	🤱	—	—	—	—	breast-feeding
person-fantasy
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
325	U+1F47C	👼	👼	👼	👼	👼	👼	👼	👼	👼	👼	—	👼	baby angel
326	U+1F385	🎅	🎅	🎅	🎅	🎅	🎅	🎅	🎅	🎅	🎅	—	🎅	Santa Claus
327	U+1F936	🤶	🤶	🤶	🤶	🤶	🤶	🤶	🤶	—	—	—	—	Mrs. Claus
328	U+1F9B8	🦸	🦸	🦸	🦸	🦸	🦸	🦸	🦸	—	—	—	—	superhero
329	U+1F9B8 U+200D U+2642 U+FE0F	🦸‍♂️	🦸‍♂️	🦸‍♂️	🦸‍♂️	🦸‍♂️	🦸‍♂️	🦸‍♂️	🦸‍♂️	—	—	—	—	man superhero
330	U+1F9B8 U+200D U+2640 U+FE0F	🦸‍♀️	🦸‍♀️	🦸‍♀️	🦸‍♀️	🦸‍♀️	🦸‍♀️	🦸‍♀️	🦸‍♀️	—	—	—	—	woman superhero
331	U+1F9B9	🦹	🦹	🦹	🦹	🦹	🦹	🦹	🦹	—	—	—	—	supervillain
332	U+1F9B9 U+200D U+2642 U+FE0F	🦹‍♂️	🦹‍♂️	🦹‍♂️	🦹‍♂️	🦹‍♂️	🦹‍♂️	🦹‍♂️	🦹‍♂️	—	—	—	—	man supervillain
333	U+1F9B9 U+200D U+2640 U+FE0F	🦹‍♀️	🦹‍♀️	🦹‍♀️	🦹‍♀️	🦹‍♀️	🦹‍♀️	🦹‍♀️	🦹‍♀️	—	—	—	—	woman supervillain
334	U+1F9D9	🧙	🧙	🧙	🧙	🧙	🧙	🧙	🧙	—	—	—	—	mage
335	U+1F9D9 U+200D U+2642 U+FE0F	🧙‍♂️	🧙‍♂️	🧙‍♂️	🧙‍♂️	🧙‍♂️	🧙‍♂️	🧙‍♂️	🧙‍♂️	—	—	—	—	man mage
336	U+1F9D9 U+200D U+2640 U+FE0F	🧙‍♀️	🧙‍♀️	🧙‍♀️	🧙‍♀️	🧙‍♀️	🧙‍♀️	🧙‍♀️	🧙‍♀️	—	—	—	—	woman mage
337	U+1F9DA	🧚	🧚	🧚	🧚	🧚	🧚	🧚	🧚	—	—	—	—	fairy
338	U+1F9DA U+200D U+2642 U+FE0F	🧚‍♂️	🧚‍♂️	🧚‍♂️	🧚‍♂️	🧚‍♂️	🧚‍♂️	🧚‍♂️	🧚‍♂️	—	—	—	—	man fairy
339	U+1F9DA U+200D U+2640 U+FE0F	🧚‍♀️	🧚‍♀️	🧚‍♀️	🧚‍♀️	🧚‍♀️	🧚‍♀️	🧚‍♀️	🧚‍♀️	—	—	—	—	woman fairy
340	U+1F9DB	🧛	🧛	🧛	🧛	🧛	🧛	🧛	🧛	—	—	—	—	vampire
341	U+1F9DB U+200D U+2642 U+FE0F	🧛‍♂️	🧛‍♂️	🧛‍♂️	🧛‍♂️	🧛‍♂️	🧛‍♂️	🧛‍♂️	🧛‍♂️	—	—	—	—	man vampire
342	U+1F9DB U+200D U+2640 U+FE0F	🧛‍♀️	🧛‍♀️	🧛‍♀️	🧛‍♀️	🧛‍♀️	🧛‍♀️	🧛‍♀️	🧛‍♀️	—	—	—	—	woman vampire
343	U+1F9DC	🧜	🧜	🧜	🧜	🧜	🧜	🧜	🧜	—	—	—	—	merperson
344	U+1F9DC U+200D U+2642 U+FE0F	🧜‍♂️	🧜‍♂️	🧜‍♂️	🧜‍♂️	🧜‍♂️	🧜‍♂️	🧜‍♂️	🧜‍♂️	—	—	—	—	merman
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
345	U+1F9DC U+200D U+2640 U+FE0F	🧜‍♀️	🧜‍♀️	🧜‍♀️	🧜‍♀️	🧜‍♀️	🧜‍♀️	🧜‍♀️	🧜‍♀️	—	—	—	—	mermaid
346	U+1F9DD	🧝	🧝	🧝	🧝	🧝	🧝	🧝	🧝	—	—	—	—	elf
347	U+1F9DD U+200D U+2642 U+FE0F	🧝‍♂️	🧝‍♂️	🧝‍♂️	🧝‍♂️	🧝‍♂️	🧝‍♂️	🧝‍♂️	🧝‍♂️	—	—	—	—	man elf
348	U+1F9DD U+200D U+2640 U+FE0F	🧝‍♀️	🧝‍♀️	🧝‍♀️	🧝‍♀️	🧝‍♀️	🧝‍♀️	🧝‍♀️	🧝‍♀️	—	—	—	—	woman elf
349	U+1F9DE	🧞	🧞	🧞	🧞	🧞	🧞	🧞	🧞	—	—	—	—	genie
350	U+1F9DE U+200D U+2642 U+FE0F	🧞‍♂️	🧞‍♂️	🧞‍♂️	🧞‍♂️	🧞‍♂️	🧞‍♂️	🧞‍♂️	🧞‍♂️	—	—	—	—	man genie
351	U+1F9DE U+200D U+2640 U+FE0F	🧞‍♀️	🧞‍♀️	🧞‍♀️	🧞‍♀️	🧞‍♀️	🧞‍♀️	🧞‍♀️	🧞‍♀️	—	—	—	—	woman genie
352	U+1F9DF	🧟	🧟	🧟	🧟	🧟	🧟	🧟	🧟	—	—	—	—	zombie
353	U+1F9DF U+200D U+2642 U+FE0F	🧟‍♂️	🧟‍♂️	🧟‍♂️	🧟‍♂️	🧟‍♂️	🧟‍♂️	🧟‍♂️	🧟‍♂️	—	—	—	—	man zombie
354	U+1F9DF U+200D U+2640 U+FE0F	🧟‍♀️	🧟‍♀️	🧟‍♀️	🧟‍♀️	🧟‍♀️	🧟‍♀️	🧟‍♀️	🧟‍♀️	—	—	—	—	woman zombie
person-activity
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
355	U+1F486	💆	💆	💆	💆	💆	💆	💆	💆	💆	💆	—	💆	person getting massage
356	U+1F486 U+200D U+2642 U+FE0F	💆‍♂️	💆‍♂️	💆‍♂️	💆‍♂️	💆‍♂️	💆‍♂️	💆‍♂️	💆‍♂️	—	—	—	—	man getting massage
357	U+1F486 U+200D U+2640 U+FE0F	💆‍♀️	💆‍♀️	💆‍♀️	💆‍♀️	💆‍♀️	💆‍♀️	💆‍♀️	💆‍♀️	—	—	—	—	woman getting massage
358	U+1F487	💇	💇	💇	💇	💇	💇	💇	💇	💇	💇	—	💇	person getting haircut
359	U+1F487 U+200D U+2642 U+FE0F	💇‍♂️	💇‍♂️	💇‍♂️	💇‍♂️	💇‍♂️	💇‍♂️	💇‍♂️	💇‍♂️	—	—	—	—	man getting haircut
360	U+1F487 U+200D U+2640 U+FE0F	💇‍♀️	💇‍♀️	💇‍♀️	💇‍♀️	💇‍♀️	💇‍♀️	💇‍♀️	💇‍♀️	—	—	—	—	woman getting haircut
361	U+1F6B6	🚶	🚶	🚶	🚶	🚶	🚶	🚶	🚶	🚶	🚶	—	🚶	person walking
362	U+1F6B6 U+200D U+2642 U+FE0F	🚶‍♂️	🚶‍♂️	🚶‍♂️	🚶‍♂️	🚶‍♂️	🚶‍♂️	🚶‍♂️	🚶‍♂️	—	—	—	—	man walking
363	U+1F6B6 U+200D U+2640 U+FE0F	🚶‍♀️	🚶‍♀️	🚶‍♀️	🚶‍♀️	🚶‍♀️	🚶‍♀️	🚶‍♀️	🚶‍♀️	—	—	—	—	woman walking
364	U+1F9CD	🧍	🧍	🧍	🧍	🧍	🧍	🧍	🧍	—	—	—	—	person standing
365	U+1F9CD U+200D U+2642 U+FE0F	🧍‍♂️	🧍‍♂️	🧍‍♂️	🧍‍♂️	—	🧍‍♂️	🧍‍♂️	🧍‍♂️	—	—	—	—	man standing
366	U+1F9CD U+200D U+2640 U+FE0F	🧍‍♀️	🧍‍♀️	🧍‍♀️	🧍‍♀️	—	🧍‍♀️	🧍‍♀️	🧍‍♀️	—	—	—	—	woman standing
367	U+1F9CE	🧎	🧎	🧎	🧎	🧎	🧎	🧎	🧎	—	—	—	—	person kneeling
368	U+1F9CE U+200D U+2642 U+FE0F	🧎‍♂️	🧎‍♂️	🧎‍♂️	🧎‍♂️	—	🧎‍♂️	🧎‍♂️	🧎‍♂️	—	—	—	—	man kneeling
369	U+1F9CE U+200D U+2640 U+FE0F	🧎‍♀️	🧎‍♀️	🧎‍♀️	🧎‍♀️	—	🧎‍♀️	🧎‍♀️	🧎‍♀️	—	—	—	—	woman kneeling
370	U+1F9D1 U+200D U+1F9AF	🧑‍🦯	🧑‍🦯	🧑‍🦯	—	—	🧑‍🦯	—	—	—	—	—	—	⊛ person with white cane
371	U+1F468 U+200D U+1F9AF	👨‍🦯	👨‍🦯	👨‍🦯	👨‍🦯	—	👨‍🦯	👨‍🦯	👨‍🦯	—	—	—	—	man with white cane
372	U+1F469 U+200D U+1F9AF	👩‍🦯	👩‍🦯	👩‍🦯	👩‍🦯	—	👩‍🦯	👩‍🦯	👩‍🦯	—	—	—	—	woman with white cane
373	U+1F9D1 U+200D U+1F9BC	🧑‍🦼	🧑‍🦼	🧑‍🦼	—	—	🧑‍🦼	—	—	—	—	—	—	⊛ person in motorized wheelchair
374	U+1F468 U+200D U+1F9BC	👨‍🦼	👨‍🦼	👨‍🦼	👨‍🦼	👨‍🦼	👨‍🦼	👨‍🦼	👨‍🦼	—	—	—	—	man in motorized wheelchair
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
375	U+1F469 U+200D U+1F9BC	👩‍🦼	👩‍🦼	👩‍🦼	👩‍🦼	👩‍🦼	👩‍🦼	👩‍🦼	👩‍🦼	—	—	—	—	woman in motorized wheelchair
376	U+1F9D1 U+200D U+1F9BD	🧑‍🦽	🧑‍🦽	🧑‍🦽	—	—	🧑‍🦽	—	—	—	—	—	—	⊛ person in manual wheelchair
377	U+1F468 U+200D U+1F9BD	👨‍🦽	👨‍🦽	👨‍🦽	👨‍🦽	👨‍🦽	👨‍🦽	👨‍🦽	👨‍🦽	—	—	—	—	man in manual wheelchair
378	U+1F469 U+200D U+1F9BD	👩‍🦽	👩‍🦽	👩‍🦽	👩‍🦽	👩‍🦽	👩‍🦽	👩‍🦽	👩‍🦽	—	—	—	—	woman in manual wheelchair
379	U+1F3C3	🏃	🏃	🏃	🏃	🏃	🏃	🏃	🏃	🏃	🏃	🏃	🏃	person running
380	U+1F3C3 U+200D U+2642 U+FE0F	🏃‍♂️	🏃‍♂️	🏃‍♂️	🏃‍♂️	🏃‍♂️	🏃‍♂️	🏃‍♂️	🏃‍♂️	—	—	—	—	man running
381	U+1F3C3 U+200D U+2640 U+FE0F	🏃‍♀️	🏃‍♀️	🏃‍♀️	🏃‍♀️	🏃‍♀️	🏃‍♀️	🏃‍♀️	🏃‍♀️	—	—	—	—	woman running
382	U+1F483	💃	💃	💃	💃	💃	💃	💃	💃	💃	💃	—	💃	woman dancing
383	U+1F57A	🕺	🕺	🕺	🕺	🕺	🕺	🕺	🕺	—	—	—	—	man dancing
384	U+1F574	🕴	🕴	🕴	🕴	🕴	🕴	🕴	🕴	—	—	—	—	person in suit levitating
385	U+1F46F	👯	👯	👯	👯	👯	👯	👯	👯	👯	👯	—	👯	people with bunny ears
386	U+1F46F U+200D U+2642 U+FE0F	👯‍♂️	👯‍♂️	👯‍♂️	👯‍♂️	👯‍♂️	👯‍♂️	👯‍♂️	👯‍♂️	—	—	—	—	men with bunny ears
387	U+1F46F U+200D U+2640 U+FE0F	👯‍♀️	👯‍♀️	👯‍♀️	👯‍♀️	👯‍♀️	👯‍♀️	👯‍♀️	👯‍♀️	—	—	—	—	women with bunny ears
388	U+1F9D6	🧖	🧖	🧖	🧖	🧖	🧖	🧖	🧖	—	—	—	—	person in steamy room
389	U+1F9D6 U+200D U+2642 U+FE0F	🧖‍♂️	🧖‍♂️	🧖‍♂️	🧖‍♂️	🧖‍♂️	🧖‍♂️	🧖‍♂️	🧖‍♂️	—	—	—	—	man in steamy room
390	U+1F9D6 U+200D U+2640 U+FE0F	🧖‍♀️	🧖‍♀️	🧖‍♀️	🧖‍♀️	🧖‍♀️	🧖‍♀️	🧖‍♀️	🧖‍♀️	—	—	—	—	woman in steamy room
391	U+1F9D7	🧗	🧗	🧗	🧗	🧗	🧗	🧗	🧗	—	—	—	—	person climbing
392	U+1F9D7 U+200D U+2642 U+FE0F	🧗‍♂️	🧗‍♂️	🧗‍♂️	🧗‍♂️	🧗‍♂️	🧗‍♂️	🧗‍♂️	🧗‍♂️	—	—	—	—	man climbing
393	U+1F9D7 U+200D U+2640 U+FE0F	🧗‍♀️	🧗‍♀️	🧗‍♀️	🧗‍♀️	🧗‍♀️	🧗‍♀️	🧗‍♀️	🧗‍♀️	—	—	—	—	woman climbing
person-sport
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
394	U+1F93A	🤺	🤺	🤺	🤺	🤺	🤺	🤺	🤺	—	—	—	—	person fencing
395	U+1F3C7	🏇	🏇	🏇	🏇	🏇	🏇	🏇	🏇	—	—	—	—	horse racing
396	U+26F7	⛷	⛷	⛷	⛷	⛷	⛷	⛷	⛷	—	—	—	—	skier
397	U+1F3C2	🏂	🏂	🏂	🏂	🏂	🏂	🏂	🏂	🏂	—	🏂	🏂	snowboarder
398	U+1F3CC	🏌	🏌	🏌	🏌	🏌	🏌	🏌	🏌	—	—	—	—	person golfing
399	U+1F3CC U+FE0F U+200D U+2642 U+FE0F	🏌️‍♂️	🏌️‍♂️	🏌️‍♂️	🏌️‍♂️	🏌️‍♂️	🏌️‍♂️	🏌️‍♂️	🏌️‍♂️	—	—	—	—	man golfing
400	U+1F3CC U+FE0F U+200D U+2640 U+FE0F	🏌️‍♀️	🏌️‍♀️	🏌️‍♀️	🏌️‍♀️	🏌️‍♀️	🏌️‍♀️	🏌️‍♀️	🏌️‍♀️	—	—	—	—	woman golfing
401	U+1F3C4	🏄	🏄	🏄	🏄	🏄	🏄	🏄	🏄	🏄	🏄	—	🏄	person surfing
402	U+1F3C4 U+200D U+2642 U+FE0F	🏄‍♂️	🏄‍♂️	🏄‍♂️	🏄‍♂️	🏄‍♂️	🏄‍♂️	🏄‍♂️	🏄‍♂️	—	—	—	—	man surfing
403	U+1F3C4 U+200D U+2640 U+FE0F	🏄‍♀️	🏄‍♀️	🏄‍♀️	🏄‍♀️	🏄‍♀️	🏄‍♀️	🏄‍♀️	🏄‍♀️	—	—	—	—	woman surfing
404	U+1F6A3	🚣	🚣	🚣	🚣	🚣	🚣	🚣	🚣	—	—	—	—	person rowing boat
405	U+1F6A3 U+200D U+2642 U+FE0F	🚣‍♂️	🚣‍♂️	🚣‍♂️	🚣‍♂️	🚣‍♂️	🚣‍♂️	🚣‍♂️	🚣‍♂️	—	—	—	—	man rowing boat
406	U+1F6A3 U+200D U+2640 U+FE0F	🚣‍♀️	🚣‍♀️	🚣‍♀️	🚣‍♀️	🚣‍♀️	🚣‍♀️	🚣‍♀️	🚣‍♀️	—	—	—	—	woman rowing boat
407	U+1F3CA	🏊	🏊	🏊	🏊	🏊	🏊	🏊	🏊	🏊	🏊	—	🏊	person swimming
408	U+1F3CA U+200D U+2642 U+FE0F	🏊‍♂️	🏊‍♂️	🏊‍♂️	🏊‍♂️	🏊‍♂️	🏊‍♂️	🏊‍♂️	🏊‍♂️	—	—	—	—	man swimming
409	U+1F3CA U+200D U+2640 U+FE0F	🏊‍♀️	🏊‍♀️	🏊‍♀️	🏊‍♀️	🏊‍♀️	🏊‍♀️	🏊‍♀️	🏊‍♀️	—	—	—	—	woman swimming
410	U+26F9	⛹	⛹	⛹	⛹	⛹	⛹	⛹	⛹	—	—	—	—	person bouncing ball
411	U+26F9 U+FE0F U+200D U+2642 U+FE0F	⛹️‍♂️	⛹️‍♂️	⛹️‍♂️	⛹️‍♂️	⛹️‍♂️	⛹️‍♂️	⛹️‍♂️	⛹️‍♂️	—	—	—	—	man bouncing ball
412	U+26F9 U+FE0F U+200D U+2640 U+FE0F	⛹️‍♀️	⛹️‍♀️	⛹️‍♀️	⛹️‍♀️	⛹️‍♀️	⛹️‍♀️	⛹️‍♀️	⛹️‍♀️	—	—	—	—	woman bouncing ball
413	U+1F3CB	🏋	🏋	🏋	🏋	🏋	🏋	🏋	🏋	—	—	—	—	person lifting weights
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
414	U+1F3CB U+FE0F U+200D U+2642 U+FE0F	🏋️‍♂️	🏋️‍♂️	🏋️‍♂️	🏋️‍♂️	🏋️‍♂️	🏋️‍♂️	🏋️‍♂️	🏋️‍♂️	—	—	—	—	man lifting weights
415	U+1F3CB U+FE0F U+200D U+2640 U+FE0F	🏋️‍♀️	🏋️‍♀️	🏋️‍♀️	🏋️‍♀️	🏋️‍♀️	🏋️‍♀️	🏋️‍♀️	🏋️‍♀️	—	—	—	—	woman lifting weights
416	U+1F6B4	🚴	🚴	🚴	🚴	🚴	🚴	🚴	🚴	—	—	—	—	person biking
417	U+1F6B4 U+200D U+2642 U+FE0F	🚴‍♂️	🚴‍♂️	🚴‍♂️	🚴‍♂️	🚴‍♂️	🚴‍♂️	🚴‍♂️	🚴‍♂️	—	—	—	—	man biking
418	U+1F6B4 U+200D U+2640 U+FE0F	🚴‍♀️	🚴‍♀️	🚴‍♀️	🚴‍♀️	🚴‍♀️	🚴‍♀️	🚴‍♀️	🚴‍♀️	—	—	—	—	woman biking
419	U+1F6B5	🚵	🚵	🚵	🚵	🚵	🚵	🚵	🚵	—	—	—	—	person mountain biking
420	U+1F6B5 U+200D U+2642 U+FE0F	🚵‍♂️	🚵‍♂️	🚵‍♂️	🚵‍♂️	🚵‍♂️	🚵‍♂️	🚵‍♂️	🚵‍♂️	—	—	—	—	man mountain biking
421	U+1F6B5 U+200D U+2640 U+FE0F	🚵‍♀️	🚵‍♀️	🚵‍♀️	🚵‍♀️	🚵‍♀️	🚵‍♀️	🚵‍♀️	🚵‍♀️	—	—	—	—	woman mountain biking
422	U+1F938	🤸	🤸	🤸	🤸	🤸	🤸	🤸	🤸	—	—	—	—	person cartwheeling
423	U+1F938 U+200D U+2642 U+FE0F	🤸‍♂️	🤸‍♂️	🤸‍♂️	🤸‍♂️	🤸‍♂️	🤸‍♂️	🤸‍♂️	🤸‍♂️	—	—	—	—	man cartwheeling
424	U+1F938 U+200D U+2640 U+FE0F	🤸‍♀️	🤸‍♀️	🤸‍♀️	🤸‍♀️	🤸‍♀️	🤸‍♀️	🤸‍♀️	🤸‍♀️	—	—	—	—	woman cartwheeling
425	U+1F93C	🤼	🤼	🤼	🤼	🤼	🤼	🤼	🤼	—	—	—	—	people wrestling
426	U+1F93C U+200D U+2642 U+FE0F	🤼‍♂️	🤼‍♂️	🤼‍♂️	🤼‍♂️	🤼‍♂️	🤼‍♂️	🤼‍♂️	🤼‍♂️	—	—	—	—	men wrestling
427	U+1F93C U+200D U+2640 U+FE0F	🤼‍♀️	🤼‍♀️	🤼‍♀️	🤼‍♀️	🤼‍♀️	🤼‍♀️	🤼‍♀️	🤼‍♀️	—	—	—	—	women wrestling
428	U+1F93D	🤽	🤽	🤽	🤽	🤽	🤽	🤽	🤽	—	—	—	—	person playing water polo
429	U+1F93D U+200D U+2642 U+FE0F	🤽‍♂️	🤽‍♂️	🤽‍♂️	🤽‍♂️	🤽‍♂️	🤽‍♂️	🤽‍♂️	🤽‍♂️	—	—	—	—	man playing water polo
430	U+1F93D U+200D U+2640 U+FE0F	🤽‍♀️	🤽‍♀️	🤽‍♀️	🤽‍♀️	🤽‍♀️	🤽‍♀️	🤽‍♀️	🤽‍♀️	—	—	—	—	woman playing water polo
431	U+1F93E	🤾	🤾	🤾	🤾	🤾	🤾	🤾	🤾	—	—	—	—	person playing handball
432	U+1F93E U+200D U+2642 U+FE0F	🤾‍♂️	🤾‍♂️	🤾‍♂️	🤾‍♂️	🤾‍♂️	🤾‍♂️	🤾‍♂️	🤾‍♂️	—	—	—	—	man playing handball
433	U+1F93E U+200D U+2640 U+FE0F	🤾‍♀️	🤾‍♀️	🤾‍♀️	🤾‍♀️	🤾‍♀️	🤾‍♀️	🤾‍♀️	🤾‍♀️	—	—	—	—	woman playing handball
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
434	U+1F939	🤹	🤹	🤹	🤹	🤹	🤹	🤹	🤹	—	—	—	—	person juggling
435	U+1F939 U+200D U+2642 U+FE0F	🤹‍♂️	🤹‍♂️	🤹‍♂️	🤹‍♂️	🤹‍♂️	🤹‍♂️	🤹‍♂️	🤹‍♂️	—	—	—	—	man juggling
436	U+1F939 U+200D U+2640 U+FE0F	🤹‍♀️	🤹‍♀️	🤹‍♀️	🤹‍♀️	🤹‍♀️	🤹‍♀️	🤹‍♀️	🤹‍♀️	—	—	—	—	woman juggling
person-resting
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
437	U+1F9D8	🧘	🧘	🧘	🧘	🧘	🧘	🧘	🧘	—	—	—	—	person in lotus position
438	U+1F9D8 U+200D U+2642 U+FE0F	🧘‍♂️	🧘‍♂️	🧘‍♂️	🧘‍♂️	🧘‍♂️	🧘‍♂️	🧘‍♂️	🧘‍♂️	—	—	—	—	man in lotus position
439	U+1F9D8 U+200D U+2640 U+FE0F	🧘‍♀️	🧘‍♀️	🧘‍♀️	🧘‍♀️	🧘‍♀️	🧘‍♀️	🧘‍♀️	🧘‍♀️	—	—	—	—	woman in lotus position
440	U+1F6C0	🛀	🛀	🛀	🛀	🛀	🛀	🛀	🛀	🛀	🛀	—	🛀	person taking bath
441	U+1F6CC	🛌	🛌	🛌	🛌	🛌	🛌	🛌	🛌	—	—	—	—	person in bed
family
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
442	U+1F9D1 U+200D U+1F91D U+200D U+1F9D1	🧑‍🤝‍🧑	🧑‍🤝‍🧑	🧑‍🤝‍🧑	🧑‍🤝‍🧑	🧑‍🤝‍🧑	🧑‍🤝‍🧑	🧑‍🤝‍🧑	🧑‍🤝‍🧑	—	—	—	—	people holding hands
443	U+1F46D	👭	👭	👭	👭	👭	👭	👭	👭	—	—	—	—	women holding hands
444	U+1F46B	👫	👫	👫	👫	👫	👫	👫	👫	👫	👫	—	—	woman and man holding hands
445	U+1F46C	👬	👬	👬	👬	👬	👬	👬	👬	—	—	—	—	men holding hands
446	U+1F48F	💏	💏	💏	💏	💏	💏	💏	💏	💏	💏	—	💏	kiss
447	U+1F469 U+200D U+2764 U+FE0F U+200D U+1F48B U+200D U+1F468	👩‍❤️‍💋‍👨	👩‍❤️‍💋‍👨	👩‍❤️‍💋‍👨	👩‍❤️‍💋‍👨	👩‍❤️‍💋‍👨	👩‍❤️‍💋‍👨	👩‍❤️‍💋‍👨	👩‍❤️‍💋‍👨	—	—	—	—	kiss: woman, man
448	U+1F468 U+200D U+2764 U+FE0F U+200D U+1F48B U+200D U+1F468	👨‍❤️‍💋‍👨	👨‍❤️‍💋‍👨	👨‍❤️‍💋‍👨	👨‍❤️‍💋‍👨	👨‍❤️‍💋‍👨	👨‍❤️‍💋‍👨	👨‍❤️‍💋‍👨	👨‍❤️‍💋‍👨	—	—	—	—	kiss: man, man
449	U+1F469 U+200D U+2764 U+FE0F U+200D U+1F48B U+200D U+1F469	👩‍❤️‍💋‍👩	👩‍❤️‍💋‍👩	👩‍❤️‍💋‍👩	👩‍❤️‍💋‍👩	👩‍❤️‍💋‍👩	👩‍❤️‍💋‍👩	👩‍❤️‍💋‍👩	👩‍❤️‍💋‍👩	—	—	—	—	kiss: woman, woman
450	U+1F491	💑	💑	💑	💑	💑	💑	💑	💑	💑	💑	—	💑	couple with heart
451	U+1F469 U+200D U+2764 U+FE0F U+200D U+1F468	👩‍❤️‍👨	👩‍❤️‍👨	👩‍❤️‍👨	👩‍❤️‍👨	👩‍❤️‍👨	👩‍❤️‍👨	👩‍❤️‍👨	👩‍❤️‍👨	—	—	—	—	couple with heart: woman, man
452	U+1F468 U+200D U+2764 U+FE0F U+200D U+1F468	👨‍❤️‍👨	👨‍❤️‍👨	👨‍❤️‍👨	👨‍❤️‍👨	👨‍❤️‍👨	👨‍❤️‍👨	👨‍❤️‍👨	👨‍❤️‍👨	—	—	—	—	couple with heart: man, man
453	U+1F469 U+200D U+2764 U+FE0F U+200D U+1F469	👩‍❤️‍👩	👩‍❤️‍👩	👩‍❤️‍👩	👩‍❤️‍👩	👩‍❤️‍👩	👩‍❤️‍👩	👩‍❤️‍👩	👩‍❤️‍👩	—	—	—	—	couple with heart: woman, woman
454	U+1F46A	👪	👪	👪	👪	👪	👪	👪	👪	👪	—	—	👪	family
455	U+1F468 U+200D U+1F469 U+200D U+1F466	👨‍👩‍👦	👨‍👩‍👦	👨‍👩‍👦	👨‍👩‍👦	👨‍👩‍👦	👨‍👩‍👦	👨‍👩‍👦	👨‍👩‍👦	—	—	—	—	family: man, woman, boy
456	U+1F468 U+200D U+1F469 U+200D U+1F467	👨‍👩‍👧	👨‍👩‍👧	👨‍👩‍👧	👨‍👩‍👧	👨‍👩‍👧	👨‍👩‍👧	👨‍👩‍👧	👨‍👩‍👧	—	—	—	—	family: man, woman, girl
457	U+1F468 U+200D U+1F469 U+200D U+1F467 U+200D U+1F466	👨‍👩‍👧‍👦	👨‍👩‍👧‍👦	👨‍👩‍👧‍👦	👨‍👩‍👧‍👦	👨‍👩‍👧‍👦	👨‍👩‍👧‍👦	👨‍👩‍👧‍👦	👨‍👩‍👧‍👦	—	—	—	—	family: man, woman, girl, boy
458	U+1F468 U+200D U+1F469 U+200D U+1F466 U+200D U+1F466	👨‍👩‍👦‍👦	👨‍👩‍👦‍👦	👨‍👩‍👦‍👦	👨‍👩‍👦‍👦	👨‍👩‍👦‍👦	👨‍👩‍👦‍👦	👨‍👩‍👦‍👦	👨‍👩‍👦‍👦	—	—	—	—	family: man, woman, boy, boy
459	U+1F468 U+200D U+1F469 U+200D U+1F467 U+200D U+1F467	👨‍👩‍👧‍👧	👨‍👩‍👧‍👧	👨‍👩‍👧‍👧	👨‍👩‍👧‍👧	👨‍👩‍👧‍👧	👨‍👩‍👧‍👧	👨‍👩‍👧‍👧	👨‍👩‍👧‍👧	—	—	—	—	family: man, woman, girl, girl
460	U+1F468 U+200D U+1F468 U+200D U+1F466	👨‍👨‍👦	👨‍👨‍👦	👨‍👨‍👦	👨‍👨‍👦	👨‍👨‍👦	👨‍👨‍👦	👨‍👨‍👦	👨‍👨‍👦	—	—	—	—	family: man, man, boy
461	U+1F468 U+200D U+1F468 U+200D U+1F467	👨‍👨‍👧	👨‍👨‍👧	👨‍👨‍👧	👨‍👨‍👧	👨‍👨‍👧	👨‍👨‍👧	👨‍👨‍👧	👨‍👨‍👧	—	—	—	—	family: man, man, girl
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
462	U+1F468 U+200D U+1F468 U+200D U+1F467 U+200D U+1F466	👨‍👨‍👧‍👦	👨‍👨‍👧‍👦	👨‍👨‍👧‍👦	👨‍👨‍👧‍👦	👨‍👨‍👧‍👦	👨‍👨‍👧‍👦	👨‍👨‍👧‍👦	👨‍👨‍👧‍👦	—	—	—	—	family: man, man, girl, boy
463	U+1F468 U+200D U+1F468 U+200D U+1F466 U+200D U+1F466	👨‍👨‍👦‍👦	👨‍👨‍👦‍👦	👨‍👨‍👦‍👦	👨‍👨‍👦‍👦	👨‍👨‍👦‍👦	👨‍👨‍👦‍👦	👨‍👨‍👦‍👦	👨‍👨‍👦‍👦	—	—	—	—	family: man, man, boy, boy
464	U+1F468 U+200D U+1F468 U+200D U+1F467 U+200D U+1F467	👨‍👨‍👧‍👧	👨‍👨‍👧‍👧	👨‍👨‍👧‍👧	👨‍👨‍👧‍👧	👨‍👨‍👧‍👧	👨‍👨‍👧‍👧	👨‍👨‍👧‍👧	👨‍👨‍👧‍👧	—	—	—	—	family: man, man, girl, girl
465	U+1F469 U+200D U+1F469 U+200D U+1F466	👩‍👩‍👦	👩‍👩‍👦	👩‍👩‍👦	👩‍👩‍👦	👩‍👩‍👦	👩‍👩‍👦	👩‍👩‍👦	👩‍👩‍👦	—	—	—	—	family: woman, woman, boy
466	U+1F469 U+200D U+1F469 U+200D U+1F467	👩‍👩‍👧	👩‍👩‍👧	👩‍👩‍👧	👩‍👩‍👧	👩‍👩‍👧	👩‍👩‍👧	👩‍👩‍👧	👩‍👩‍👧	—	—	—	—	family: woman, woman, girl
467	U+1F469 U+200D U+1F469 U+200D U+1F467 U+200D U+1F466	👩‍👩‍👧‍👦	👩‍👩‍👧‍👦	👩‍👩‍👧‍👦	👩‍👩‍👧‍👦	👩‍👩‍👧‍👦	👩‍👩‍👧‍👦	👩‍👩‍👧‍👦	👩‍👩‍👧‍👦	—	—	—	—	family: woman, woman, girl, boy
468	U+1F469 U+200D U+1F469 U+200D U+1F466 U+200D U+1F466	👩‍👩‍👦‍👦	👩‍👩‍👦‍👦	👩‍👩‍👦‍👦	👩‍👩‍👦‍👦	👩‍👩‍👦‍👦	👩‍👩‍👦‍👦	👩‍👩‍👦‍👦	👩‍👩‍👦‍👦	—	—	—	—	family: woman, woman, boy, boy
469	U+1F469 U+200D U+1F469 U+200D U+1F467 U+200D U+1F467	👩‍👩‍👧‍👧	👩‍👩‍👧‍👧	👩‍👩‍👧‍👧	👩‍👩‍👧‍👧	👩‍👩‍👧‍👧	👩‍👩‍👧‍👧	👩‍👩‍👧‍👧	👩‍👩‍👧‍👧	—	—	—	—	family: woman, woman, girl, girl
470	U+1F468 U+200D U+1F466	👨‍👦	👨‍👦	👨‍👦	👨‍👦	👨‍👦	👨‍👦	👨‍👦	👨‍👦	—	—	—	—	family: man, boy
471	U+1F468 U+200D U+1F466 U+200D U+1F466	👨‍👦‍👦	👨‍👦‍👦	👨‍👦‍👦	👨‍👦‍👦	👨‍👦‍👦	👨‍👦‍👦	👨‍👦‍👦	👨‍👦‍👦	—	—	—	—	family: man, boy, boy
472	U+1F468 U+200D U+1F467	👨‍👧	👨‍👧	👨‍👧	👨‍👧	👨‍👧	👨‍👧	👨‍👧	👨‍👧	—	—	—	—	family: man, girl
473	U+1F468 U+200D U+1F467 U+200D U+1F466	👨‍👧‍👦	👨‍👧‍👦	👨‍👧‍👦	👨‍👧‍👦	👨‍👧‍👦	👨‍👧‍👦	👨‍👧‍👦	👨‍👧‍👦	—	—	—	—	family: man, girl, boy
474	U+1F468 U+200D U+1F467 U+200D U+1F467	👨‍👧‍👧	👨‍👧‍👧	👨‍👧‍👧	👨‍👧‍👧	👨‍👧‍👧	👨‍👧‍👧	👨‍👧‍👧	👨‍👧‍👧	—	—	—	—	family: man, girl, girl
475	U+1F469 U+200D U+1F466	👩‍👦	👩‍👦	👩‍👦	👩‍👦	👩‍👦	👩‍👦	👩‍👦	👩‍👦	—	—	—	—	family: woman, boy
476	U+1F469 U+200D U+1F466 U+200D U+1F466	👩‍👦‍👦	👩‍👦‍👦	👩‍👦‍👦	👩‍👦‍👦	👩‍👦‍👦	👩‍👦‍👦	👩‍👦‍👦	👩‍👦‍👦	—	—	—	—	family: woman, boy, boy
477	U+1F469 U+200D U+1F467	👩‍👧	👩‍👧	👩‍👧	👩‍👧	👩‍👧	👩‍👧	👩‍👧	👩‍👧	—	—	—	—	family: woman, girl
478	U+1F469 U+200D U+1F467 U+200D U+1F466	👩‍👧‍👦	👩‍👧‍👦	👩‍👧‍👦	👩‍👧‍👦	👩‍👧‍👦	👩‍👧‍👦	👩‍👧‍👦	👩‍👧‍👦	—	—	—	—	family: woman, girl, boy
479	U+1F469 U+200D U+1F467 U+200D U+1F467	👩‍👧‍👧	👩‍👧‍👧	👩‍👧‍👧	👩‍👧‍👧	👩‍👧‍👧	👩‍👧‍👧	👩‍👧‍👧	👩‍👧‍👧	—	—	—	—	family: woman, girl, girl
person-symbol
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
480	U+1F5E3	🗣	🗣	🗣	🗣	🗣	🗣	🗣	🗣	—	—	—	—	speaking head
481	U+1F464	👤	👤	👤	👤	👤	👤	👤	👤	👤	—	👤	—	bust in silhouette
482	U+1F465	👥	👥	👥	👥	👥	👥	👥	👥	—	—	—	—	busts in silhouette
483	U+1F463	👣	👣	👣	👣	👣	👣	👣	👣	👣	👣	👣	👣	footprints
Component
hair-style
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
484	U+1F9B0	🦰	🦰	🦰	🦰	🦰	🦰	🦰	🦰	—	—	—	—	red hair
485	U+1F9B1	🦱	🦱	🦱	🦱	🦱	🦱	🦱	🦱	—	—	—	—	curly hair
486	U+1F9B3	🦳	🦳	🦳	🦳	🦳	🦳	🦳	🦳	—	—	—	—	white hair
487	U+1F9B2	🦲	🦲	🦲	🦲	🦲	🦲	🦲	🦲	—	—	—	—	bald
Animals & Nature
animal-mammal
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
488	U+1F435	🐵	🐵	🐵	🐵	🐵	🐵	🐵	🐵	🐵	🐵	—	🐵	monkey face
489	U+1F412	🐒	🐒	🐒	🐒	🐒	🐒	🐒	🐒	🐒	🐒	—	—	monkey
490	U+1F98D	🦍	🦍	🦍	🦍	🦍	🦍	🦍	🦍	—	—	—	—	gorilla
491	U+1F9A7	🦧	🦧	🦧	🦧	🦧	🦧	🦧	🦧	—	—	—	—	orangutan
492	U+1F436	🐶	🐶	🐶	🐶	🐶	🐶	🐶	🐶	🐶	🐶	🐶	🐶	dog face
493	U+1F415	🐕	🐕	🐕	🐕	🐕	🐕	🐕	🐕	—	—	—	—	dog
494	U+1F9AE	🦮	🦮	🦮	🦮	🦮	🦮	🦮	🦮	—	—	—	—	guide dog
495	U+1F415 U+200D U+1F9BA	🐕‍🦺	🐕‍🦺	🐕‍🦺	🐕‍🦺	🐕‍🦺	🐕‍🦺	🐕‍🦺	🐕‍🦺	—	—	—	—	service dog
496	U+1F429	🐩	🐩	🐩	🐩	🐩	🐩	🐩	🐩	🐩	—	—	🐩	poodle
497	U+1F43A	🐺	🐺	🐺	🐺	🐺	🐺	🐺	🐺	🐺	🐺	—	—	wolf
498	U+1F98A	🦊	🦊	🦊	🦊	🦊	🦊	🦊	🦊	—	—	—	—	fox
499	U+1F99D	🦝	🦝	🦝	🦝	🦝	🦝	🦝	🦝	—	—	—	—	raccoon
500	U+1F431	🐱	🐱	🐱	🐱	🐱	🐱	🐱	🐱	🐱	🐱	🐱	🐱	cat face
501	U+1F408	🐈	🐈	🐈	🐈	🐈	🐈	🐈	🐈	—	—	—	—	cat
502	U+1F981	🦁	🦁	🦁	🦁	🦁	🦁	🦁	🦁	—	—	—	—	lion
503	U+1F42F	🐯	🐯	🐯	🐯	🐯	🐯	🐯	🐯	🐯	🐯	—	🐯	tiger face
504	U+1F405	🐅	🐅	🐅	🐅	🐅	🐅	🐅	🐅	—	—	—	—	tiger
505	U+1F406	🐆	🐆	🐆	🐆	🐆	🐆	🐆	🐆	—	—	—	—	leopard
506	U+1F434	🐴	🐴	🐴	🐴	🐴	🐴	🐴	🐴	🐴	🐴	🐴	🐴	horse face
507	U+1F40E	🐎	🐎	🐎	🐎	🐎	🐎	🐎	🐎	🐎	🐎	—	—	horse
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
508	U+1F984	🦄	🦄	🦄	🦄	🦄	🦄	🦄	🦄	—	—	—	—	unicorn
509	U+1F993	🦓	🦓	🦓	🦓	🦓	🦓	🦓	🦓	—	—	—	—	zebra
510	U+1F98C	🦌	🦌	🦌	🦌	🦌	🦌	🦌	🦌	—	—	—	—	deer
511	U+1F42E	🐮	🐮	🐮	🐮	🐮	🐮	🐮	🐮	🐮	🐮	—	🐮	cow face
512	U+1F402	🐂	🐂	🐂	🐂	🐂	🐂	🐂	🐂	—	—	—	—	ox
513	U+1F403	🐃	🐃	🐃	🐃	🐃	🐃	🐃	🐃	—	—	—	—	water buffalo
514	U+1F404	🐄	🐄	🐄	🐄	🐄	🐄	🐄	🐄	—	—	—	—	cow
515	U+1F437	🐷	🐷	🐷	🐷	🐷	🐷	🐷	🐷	🐷	🐷	🐷	🐷	pig face
516	U+1F416	🐖	🐖	🐖	🐖	🐖	🐖	🐖	🐖	—	—	—	—	pig
517	U+1F417	🐗	🐗	🐗	🐗	🐗	🐗	🐗	🐗	🐗	🐗	—	🐗	boar
518	U+1F43D	🐽	🐽	🐽	🐽	🐽	🐽	🐽	🐽	🐽	—	—	🐽	pig nose
519	U+1F40F	🐏	🐏	🐏	🐏	🐏	🐏	🐏	🐏	—	—	—	—	ram
520	U+1F411	🐑	🐑	🐑	🐑	🐑	🐑	🐑	🐑	🐑	🐑	—	—	ewe
521	U+1F410	🐐	🐐	🐐	🐐	🐐	🐐	🐐	🐐	—	—	—	—	goat
522	U+1F42A	🐪	🐪	🐪	🐪	🐪	🐪	🐪	🐪	—	—	—	—	camel
523	U+1F42B	🐫	🐫	🐫	🐫	🐫	🐫	🐫	🐫	🐫	🐫	—	🐫	two-hump camel
524	U+1F999	🦙	🦙	🦙	🦙	🦙	🦙	🦙	🦙	—	—	—	—	llama
525	U+1F992	🦒	🦒	🦒	🦒	🦒	🦒	🦒	🦒	—	—	—	—	giraffe
526	U+1F418	🐘	🐘	🐘	🐘	🐘	🐘	🐘	🐘	🐘	🐘	—	🐘	elephant
527	U+1F98F	🦏	🦏	🦏	🦏	🦏	🦏	🦏	🦏	—	—	—	—	rhinoceros
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
528	U+1F99B	🦛	🦛	🦛	🦛	🦛	🦛	🦛	🦛	—	—	—	—	hippopotamus
529	U+1F42D	🐭	🐭	🐭	🐭	🐭	🐭	🐭	🐭	🐭	🐭	—	🐭	mouse face
530	U+1F401	🐁	🐁	🐁	🐁	🐁	🐁	🐁	🐁	—	—	—	—	mouse
531	U+1F400	🐀	🐀	🐀	🐀	🐀	🐀	🐀	🐀	—	—	—	—	rat
532	U+1F439	🐹	🐹	🐹	🐹	🐹	🐹	🐹	🐹	🐹	🐹	—	—	hamster
533	U+1F430	🐰	🐰	🐰	🐰	🐰	🐰	🐰	🐰	🐰	🐰	—	🐰	rabbit face
534	U+1F407	🐇	🐇	🐇	🐇	🐇	🐇	🐇	🐇	—	—	—	—	rabbit
535	U+1F43F	🐿	🐿	🐿	🐿	🐿	🐿	🐿	🐿	—	—	—	—	chipmunk
536	U+1F994	🦔	🦔	🦔	🦔	🦔	🦔	🦔	🦔	—	—	—	—	hedgehog
537	U+1F987	🦇	🦇	🦇	🦇	🦇	🦇	🦇	🦇	—	—	—	—	bat
538	U+1F43B	🐻	🐻	🐻	🐻	🐻	🐻	🐻	🐻	🐻	🐻	—	🐻	bear
539	U+1F428	🐨	🐨	🐨	🐨	🐨	🐨	🐨	🐨	🐨	🐨	—	🐨	koala
540	U+1F43C	🐼	🐼	🐼	🐼	🐼	🐼	🐼	🐼	🐼	—	—	🐼	panda
541	U+1F9A5	🦥	🦥	🦥	🦥	🦥	🦥	🦥	🦥	—	—	—	—	sloth
542	U+1F9A6	🦦	🦦	🦦	🦦	🦦	🦦	🦦	🦦	—	—	—	—	otter
543	U+1F9A8	🦨	🦨	🦨	🦨	🦨	🦨	🦨	🦨	—	—	—	—	skunk
544	U+1F998	🦘	🦘	🦘	🦘	🦘	🦘	🦘	🦘	—	—	—	—	kangaroo
545	U+1F9A1	🦡	🦡	🦡	🦡	🦡	🦡	🦡	🦡	—	—	—	—	badger
546	U+1F43E	🐾	🐾	🐾	🐾	🐾	🐾	🐾	🐾	🐾	—	—	🐾	paw prints
animal-bird
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
547	U+1F983	🦃	🦃	🦃	🦃	🦃	🦃	🦃	🦃	—	—	—	—	turkey
548	U+1F414	🐔	🐔	🐔	🐔	🐔	🐔	🐔	🐔	🐔	🐔	—	🐔	chicken
549	U+1F413	🐓	🐓	🐓	🐓	🐓	🐓	🐓	🐓	—	—	—	—	rooster
550	U+1F423	🐣	🐣	🐣	🐣	🐣	🐣	🐣	🐣	🐣	—	—	🐣	hatching chick
551	U+1F424	🐤	🐤	🐤	🐤	🐤	🐤	🐤	🐤	🐤	🐤	🐤	🐤	baby chick
552	U+1F425	🐥	🐥	🐥	🐥	🐥	🐥	🐥	🐥	🐥	—	—	🐥	front-facing baby chick
553	U+1F426	🐦	🐦	🐦	🐦	🐦	🐦	🐦	🐦	🐦	🐦	—	—	bird
554	U+1F427	🐧	🐧	🐧	🐧	🐧	🐧	🐧	🐧	🐧	🐧	🐧	🐧	penguin
555	U+1F54A	🕊	🕊	🕊	🕊	🕊	🕊	🕊	🕊	—	—	—	—	dove
556	U+1F985	🦅	🦅	🦅	🦅	🦅	🦅	🦅	🦅	—	—	—	—	eagle
557	U+1F986	🦆	🦆	🦆	🦆	🦆	🦆	🦆	🦆	—	—	—	—	duck
558	U+1F9A2	🦢	🦢	🦢	🦢	🦢	🦢	🦢	🦢	—	—	—	—	swan
559	U+1F989	🦉	🦉	🦉	🦉	🦉	🦉	🦉	🦉	—	—	—	—	owl
560	U+1F9A9	🦩	🦩	🦩	🦩	🦩	🦩	🦩	🦩	—	—	—	—	flamingo
561	U+1F99A	🦚	🦚	🦚	🦚	🦚	🦚	🦚	🦚	—	—	—	—	peacock
562	U+1F99C	🦜	🦜	🦜	🦜	🦜	🦜	🦜	🦜	—	—	—	—	parrot
animal-amphibian
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
563	U+1F438	🐸	🐸	🐸	🐸	🐸	🐸	🐸	🐸	🐸	🐸	—	🐸	frog
animal-reptile
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
564	U+1F40A	🐊	🐊	🐊	🐊	🐊	🐊	🐊	🐊	—	—	—	—	crocodile
565	U+1F422	🐢	🐢	🐢	🐢	🐢	🐢	🐢	🐢	🐢	—	—	🐢	turtle
566	U+1F98E	🦎	🦎	🦎	🦎	🦎	🦎	🦎	🦎	—	—	—	—	lizard
567	U+1F40D	🐍	🐍	🐍	🐍	🐍	🐍	🐍	🐍	🐍	🐍	—	🐍	snake
568	U+1F432	🐲	🐲	🐲	🐲	🐲	🐲	🐲	🐲	🐲	—	—	🐲	dragon face
569	U+1F409	🐉	🐉	🐉	🐉	🐉	🐉	🐉	🐉	—	—	—	—	dragon
570	U+1F995	🦕	🦕	🦕	🦕	🦕	🦕	🦕	🦕	—	—	—	—	sauropod
571	U+1F996	🦖	🦖	🦖	🦖	🦖	🦖	🦖	🦖	—	—	—	—	T-Rex
animal-marine
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
572	U+1F433	🐳	🐳	🐳	🐳	🐳	🐳	🐳	🐳	🐳	🐳	—	🐳	spouting whale
573	U+1F40B	🐋	🐋	🐋	🐋	🐋	🐋	🐋	🐋	—	—	—	—	whale
574	U+1F42C	🐬	🐬	🐬	🐬	🐬	🐬	🐬	🐬	🐬	🐬	—	🐬	dolphin
575	U+1F41F	🐟	🐟	🐟	🐟	🐟	🐟	🐟	🐟	🐟	🐟	🐟	—	fish
576	U+1F420	🐠	🐠	🐠	🐠	🐠	🐠	🐠	🐠	🐠	🐠	—	🐠	tropical fish
577	U+1F421	🐡	🐡	🐡	🐡	🐡	🐡	🐡	🐡	🐡	—	—	🐡	blowfish
578	U+1F988	🦈	🦈	🦈	🦈	🦈	🦈	🦈	🦈	—	—	—	—	shark
579	U+1F419	🐙	🐙	🐙	🐙	🐙	🐙	🐙	🐙	🐙	🐙	—	🐙	octopus
580	U+1F41A	🐚	🐚	🐚	🐚	🐚	🐚	🐚	🐚	🐚	🐚	—	🐚	spiral shell
animal-bug
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
581	U+1F40C	🐌	🐌	🐌	🐌	🐌	🐌	🐌	🐌	🐌	—	🐌	🐌	snail
582	U+1F98B	🦋	🦋	🦋	🦋	🦋	🦋	🦋	🦋	—	—	—	—	butterfly
583	U+1F41B	🐛	🐛	🐛	🐛	🐛	🐛	🐛	🐛	🐛	🐛	—	🐛	bug
584	U+1F41C	🐜	🐜	🐜	🐜	🐜	🐜	🐜	🐜	🐜	—	—	🐜	ant
585	U+1F41D	🐝	🐝	🐝	🐝	🐝	🐝	🐝	🐝	🐝	—	—	🐝	honeybee
586	U+1F41E	🐞	🐞	🐞	🐞	🐞	🐞	🐞	🐞	🐞	—	—	🐞	lady beetle
587	U+1F997	🦗	🦗	🦗	🦗	🦗	🦗	🦗	🦗	—	—	—	—	cricket
588	U+1F577	🕷	🕷	🕷	🕷	🕷	🕷	🕷	🕷	—	—	—	—	spider
589	U+1F578	🕸	🕸	🕸	🕸	🕸	🕸	🕸	🕸	—	—	—	—	spider web
590	U+1F982	🦂	🦂	🦂	🦂	🦂	🦂	🦂	🦂	—	—	—	—	scorpion
591	U+1F99F	🦟	🦟	🦟	🦟	🦟	🦟	🦟	🦟	—	—	—	—	mosquito
592	U+1F9A0	🦠	🦠	🦠	🦠	🦠	🦠	🦠	🦠	—	—	—	—	microbe
plant-flower
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
593	U+1F490	💐	💐	💐	💐	💐	💐	💐	💐	💐	💐	—	💐	bouquet
594	U+1F338	🌸	🌸	🌸	🌸	🌸	🌸	🌸	🌸	🌸	🌸	🌸	🌸	cherry blossom
595	U+1F4AE	💮	💮	💮	💮	💮	💮	💮	💮	💮	—	—	💮	white flower
596	U+1F3F5	🏵	🏵	🏵	🏵	🏵	🏵	🏵	🏵	—	—	—	—	rosette
597	U+1F339	🌹	🌹	🌹	🌹	🌹	🌹	🌹	🌹	🌹	🌹	—	🌹	rose
598	U+1F940	🥀	🥀	🥀	🥀	🥀	🥀	🥀	🥀	—	—	—	—	wilted flower
599	U+1F33A	🌺	🌺	🌺	🌺	🌺	🌺	🌺	🌺	🌺	🌺	—	🌺	hibiscus
600	U+1F33B	🌻	🌻	🌻	🌻	🌻	🌻	🌻	🌻	🌻	🌻	—	🌻	sunflower
601	U+1F33C	🌼	🌼	🌼	🌼	🌼	🌼	🌼	🌼	🌼	—	—	🌼	blossom
602	U+1F337	🌷	🌷	🌷	🌷	🌷	🌷	🌷	🌷	🌷	🌷	🌷	🌷	tulip
plant-other
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
603	U+1F331	🌱	🌱	🌱	🌱	🌱	🌱	🌱	🌱	🌱	—	🌱	🌱	seedling
604	U+1F332	🌲	🌲	🌲	🌲	🌲	🌲	🌲	🌲	—	—	—	—	evergreen tree
605	U+1F333	🌳	🌳	🌳	🌳	🌳	🌳	🌳	🌳	—	—	—	—	deciduous tree
606	U+1F334	🌴	🌴	🌴	🌴	🌴	🌴	🌴	🌴	🌴	🌴	—	🌴	palm tree
607	U+1F335	🌵	🌵	🌵	🌵	🌵	🌵	🌵	🌵	🌵	🌵	—	🌵	cactus
608	U+1F33E	🌾	🌾	🌾	🌾	🌾	🌾	🌾	🌾	🌾	🌾	—	—	sheaf of rice
609	U+1F33F	🌿	🌿	🌿	🌿	🌿	🌿	🌿	🌿	🌿	—	—	🌿	herb
610	U+2618	☘	☘	☘	☘	☘	☘	☘	☘	—	—	—	—	shamrock
611	U+1F340	🍀	🍀	🍀	🍀	🍀	🍀	🍀	🍀	🍀	🍀	🍀	🍀	four leaf clover
612	U+1F341	🍁	🍁	🍁	🍁	🍁	🍁	🍁	🍁	🍁	🍁	🍁	🍁	maple leaf
613	U+1F342	🍂	🍂	🍂	🍂	🍂	🍂	🍂	🍂	🍂	🍂	—	🍂	fallen leaf
614	U+1F343	🍃	🍃	🍃	🍃	🍃	🍃	🍃	🍃	🍃	🍃	—	—	leaf fluttering in wind
Food & Drink
food-fruit
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
615	U+1F347	🍇	🍇	🍇	🍇	🍇	🍇	🍇	🍇	🍇	—	—	🍇	grapes
616	U+1F348	🍈	🍈	🍈	🍈	🍈	🍈	🍈	🍈	🍈	—	—	🍈	melon
617	U+1F349	🍉	🍉	🍉	🍉	🍉	🍉	🍉	🍉	🍉	🍉	—	🍉	watermelon
618	U+1F34A	🍊	🍊	🍊	🍊	🍊	🍊	🍊	🍊	🍊	🍊	—	🍊	tangerine
619	U+1F34B	🍋	🍋	🍋	🍋	🍋	🍋	🍋	🍋	—	—	—	—	lemon
620	U+1F34C	🍌	🍌	🍌	🍌	🍌	🍌	🍌	🍌	🍌	—	🍌	🍌	banana
621	U+1F34D	🍍	🍍	🍍	🍍	🍍	🍍	🍍	🍍	🍍	—	—	🍍	pineapple
622	U+1F96D	🥭	🥭	🥭	🥭	🥭	🥭	🥭	🥭	—	—	—	—	mango
623	U+1F34E	🍎	🍎	🍎	🍎	🍎	🍎	🍎	🍎	🍎	🍎	🍎	🍎	red apple
624	U+1F34F	🍏	🍏	🍏	🍏	🍏	🍏	🍏	🍏	🍏	—	—	🍏	green apple
625	U+1F350	🍐	🍐	🍐	🍐	🍐	🍐	🍐	🍐	—	—	—	—	pear
626	U+1F351	🍑	🍑	🍑	🍑	🍑	🍑	🍑	🍑	🍑	—	—	🍑	peach
627	U+1F352	🍒	🍒	🍒	🍒	🍒	🍒	🍒	🍒	🍒	—	🍒	🍒	cherries
628	U+1F353	🍓	🍓	🍓	🍓	🍓	🍓	🍓	🍓	🍓	🍓	—	🍓	strawberry
629	U+1F95D	🥝	🥝	🥝	🥝	🥝	🥝	🥝	🥝	—	—	—	—	kiwi fruit
630	U+1F345	🍅	🍅	🍅	🍅	🍅	🍅	🍅	🍅	🍅	🍅	—	🍅	tomato
631	U+1F965	🥥	🥥	🥥	🥥	🥥	🥥	🥥	🥥	—	—	—	—	coconut
food-vegetable
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
632	U+1F951	🥑	🥑	🥑	🥑	🥑	🥑	🥑	🥑	—	—	—	—	avocado
633	U+1F346	🍆	🍆	🍆	🍆	🍆	🍆	🍆	🍆	🍆	🍆	—	🍆	eggplant
634	U+1F954	🥔	🥔	🥔	🥔	🥔	🥔	🥔	🥔	—	—	—	—	potato
635	U+1F955	🥕	🥕	🥕	🥕	🥕	🥕	🥕	🥕	—	—	—	—	carrot
636	U+1F33D	🌽	🌽	🌽	🌽	🌽	🌽	🌽	🌽	🌽	—	—	🌽	ear of corn
637	U+1F336	🌶	🌶	🌶	🌶	🌶	🌶	🌶	🌶	—	—	—	—	hot pepper
638	U+1F952	🥒	🥒	🥒	🥒	🥒	🥒	🥒	🥒	—	—	—	—	cucumber
639	U+1F96C	🥬	🥬	🥬	🥬	🥬	🥬	🥬	🥬	—	—	—	—	leafy green
640	U+1F966	🥦	🥦	🥦	🥦	🥦	🥦	🥦	🥦	—	—	—	—	broccoli
641	U+1F9C4	🧄	🧄	🧄	🧄	🧄	🧄	🧄	🧄	—	—	—	—	garlic
642	U+1F9C5	🧅	🧅	🧅	🧅	🧅	🧅	🧅	🧅	—	—	—	—	onion
643	U+1F344	🍄	🍄	🍄	🍄	🍄	🍄	🍄	🍄	🍄	—	—	🍄	mushroom
644	U+1F95C	🥜	🥜	🥜	🥜	🥜	🥜	🥜	🥜	—	—	—	—	peanuts
645	U+1F330	🌰	🌰	🌰	🌰	🌰	🌰	🌰	🌰	🌰	—	—	🌰	chestnut
food-prepared
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
646	U+1F35E	🍞	🍞	🍞	🍞	🍞	🍞	🍞	🍞	🍞	🍞	🍞	🍞	bread
647	U+1F950	🥐	🥐	🥐	🥐	🥐	🥐	🥐	🥐	—	—	—	—	croissant
648	U+1F956	🥖	🥖	🥖	🥖	🥖	🥖	🥖	🥖	—	—	—	—	baguette bread
649	U+1F968	🥨	🥨	🥨	🥨	🥨	🥨	🥨	🥨	—	—	—	—	pretzel
650	U+1F96F	🥯	🥯	🥯	🥯	🥯	🥯	🥯	🥯	—	—	—	—	bagel
651	U+1F95E	🥞	🥞	🥞	🥞	🥞	🥞	🥞	🥞	—	—	—	—	pancakes
652	U+1F9C7	🧇	🧇	🧇	🧇	🧇	🧇	🧇	🧇	—	—	—	—	waffle
653	U+1F9C0	🧀	🧀	🧀	🧀	🧀	🧀	🧀	🧀	—	—	—	—	cheese wedge
654	U+1F356	🍖	🍖	🍖	🍖	🍖	🍖	🍖	🍖	🍖	—	—	🍖	meat on bone
655	U+1F357	🍗	🍗	🍗	🍗	🍗	🍗	🍗	🍗	🍗	—	—	🍗	poultry leg
656	U+1F969	🥩	🥩	🥩	🥩	🥩	🥩	🥩	🥩	—	—	—	—	cut of meat
657	U+1F953	🥓	🥓	🥓	🥓	🥓	🥓	🥓	🥓	—	—	—	—	bacon
658	U+1F354	🍔	🍔	🍔	🍔	🍔	🍔	🍔	🍔	🍔	🍔	🍔	🍔	hamburger
659	U+1F35F	🍟	🍟	🍟	🍟	🍟	🍟	🍟	🍟	🍟	🍟	—	🍟	french fries
660	U+1F355	🍕	🍕	🍕	🍕	🍕	🍕	🍕	🍕	🍕	—	—	🍕	pizza
661	U+1F32D	🌭	🌭	🌭	🌭	🌭	🌭	🌭	🌭	—	—	—	—	hot dog
662	U+1F96A	🥪	🥪	🥪	🥪	🥪	🥪	🥪	🥪	—	—	—	—	sandwich
663	U+1F32E	🌮	🌮	🌮	🌮	🌮	🌮	🌮	🌮	—	—	—	—	taco
664	U+1F32F	🌯	🌯	🌯	🌯	🌯	🌯	🌯	🌯	—	—	—	—	burrito
665	U+1F959	🥙	🥙	🥙	🥙	🥙	🥙	🥙	🥙	—	—	—	—	stuffed flatbread
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
666	U+1F9C6	🧆	🧆	🧆	🧆	🧆	🧆	🧆	🧆	—	—	—	—	falafel
667	U+1F95A	🥚	🥚	🥚	🥚	🥚	🥚	🥚	🥚	—	—	—	—	egg
668	U+1F373	🍳	🍳	🍳	🍳	🍳	🍳	🍳	🍳	🍳	🍳	—	🍳	cooking
669	U+1F958	🥘	🥘	🥘	🥘	🥘	🥘	🥘	🥘	—	—	—	—	shallow pan of food
670	U+1F372	🍲	🍲	🍲	🍲	🍲	🍲	🍲	🍲	🍲	🍲	—	🍲	pot of food
671	U+1F963	🥣	🥣	🥣	🥣	🥣	🥣	🥣	🥣	—	—	—	—	bowl with spoon
672	U+1F957	🥗	🥗	🥗	🥗	🥗	🥗	🥗	🥗	—	—	—	—	green salad
673	U+1F37F	🍿	🍿	🍿	🍿	🍿	🍿	🍿	🍿	—	—	—	—	popcorn
674	U+1F9C8	🧈	🧈	🧈	🧈	🧈	🧈	🧈	🧈	—	—	—	—	butter
675	U+1F9C2	🧂	🧂	🧂	🧂	🧂	🧂	🧂	🧂	—	—	—	—	salt
676	U+1F96B	🥫	🥫	🥫	🥫	🥫	🥫	🥫	🥫	—	—	—	—	canned food
food-asian
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
677	U+1F371	🍱	🍱	🍱	🍱	🍱	🍱	🍱	🍱	🍱	🍱	—	🍱	bento box
678	U+1F358	🍘	🍘	🍘	🍘	🍘	🍘	🍘	🍘	🍘	🍘	—	🍘	rice cracker
679	U+1F359	🍙	🍙	🍙	🍙	🍙	🍙	🍙	🍙	🍙	🍙	🍙	🍙	rice ball
680	U+1F35A	🍚	🍚	🍚	🍚	🍚	🍚	🍚	🍚	🍚	🍚	—	🍚	cooked rice
681	U+1F35B	🍛	🍛	🍛	🍛	🍛	🍛	🍛	🍛	🍛	🍛	—	🍛	curry rice
682	U+1F35C	🍜	🍜	🍜	🍜	🍜	🍜	🍜	🍜	🍜	🍜	🍜	🍜	steaming bowl
683	U+1F35D	🍝	🍝	🍝	🍝	🍝	🍝	🍝	🍝	🍝	🍝	—	🍝	spaghetti
684	U+1F360	🍠	🍠	🍠	🍠	🍠	🍠	🍠	🍠	🍠	—	—	🍠	roasted sweet potato
685	U+1F362	🍢	🍢	🍢	🍢	🍢	🍢	🍢	🍢	🍢	🍢	—	🍢	oden
686	U+1F363	🍣	🍣	🍣	🍣	🍣	🍣	🍣	🍣	🍣	🍣	—	🍣	sushi
687	U+1F364	🍤	🍤	🍤	🍤	🍤	🍤	🍤	🍤	🍤	—	—	🍤	fried shrimp
688	U+1F365	🍥	🍥	🍥	🍥	🍥	🍥	🍥	🍥	🍥	—	—	🍥	fish cake with swirl
689	U+1F96E	🥮	🥮	🥮	🥮	🥮	🥮	🥮	🥮	—	—	—	—	moon cake
690	U+1F361	🍡	🍡	🍡	🍡	🍡	🍡	🍡	🍡	🍡	🍡	—	🍡	dango
691	U+1F95F	🥟	🥟	🥟	🥟	🥟	🥟	🥟	🥟	—	—	—	—	dumpling
692	U+1F960	🥠	🥠	🥠	🥠	🥠	🥠	🥠	🥠	—	—	—	—	fortune cookie
693	U+1F961	🥡	🥡	🥡	🥡	🥡	🥡	🥡	🥡	—	—	—	—	takeout box
food-marine
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
694	U+1F980	🦀	🦀	🦀	🦀	🦀	🦀	🦀	🦀	—	—	—	—	crab
695	U+1F99E	🦞	🦞	🦞	🦞	🦞	🦞	🦞	🦞	—	—	—	—	lobster
696	U+1F990	🦐	🦐	🦐	🦐	🦐	🦐	🦐	🦐	—	—	—	—	shrimp
697	U+1F991	🦑	🦑	🦑	🦑	🦑	🦑	🦑	🦑	—	—	—	—	squid
698	U+1F9AA	🦪	🦪	🦪	🦪	🦪	🦪	🦪	🦪	—	—	—	—	oyster
food-sweet
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
699	U+1F366	🍦	🍦	🍦	🍦	🍦	🍦	🍦	🍦	🍦	🍦	—	🍦	soft ice cream
700	U+1F367	🍧	🍧	🍧	🍧	🍧	🍧	🍧	🍧	🍧	🍧	—	🍧	shaved ice
701	U+1F368	🍨	🍨	🍨	🍨	🍨	🍨	🍨	🍨	🍨	—	—	🍨	ice cream
702	U+1F369	🍩	🍩	🍩	🍩	🍩	🍩	🍩	🍩	🍩	—	—	🍩	doughnut
703	U+1F36A	🍪	🍪	🍪	🍪	🍪	🍪	🍪	🍪	🍪	—	—	🍪	cookie
704	U+1F382	🎂	🎂	🎂	🎂	🎂	🎂	🎂	🎂	🎂	🎂	🎂	🎂	birthday cake
705	U+1F370	🍰	🍰	🍰	🍰	🍰	🍰	🍰	🍰	🍰	🍰	🍰	🍰	shortcake
706	U+1F9C1	🧁	🧁	🧁	🧁	🧁	🧁	🧁	🧁	—	—	—	—	cupcake
707	U+1F967	🥧	🥧	🥧	🥧	🥧	🥧	🥧	🥧	—	—	—	—	pie
708	U+1F36B	🍫	🍫	🍫	🍫	🍫	🍫	🍫	🍫	🍫	—	—	🍫	chocolate bar
709	U+1F36C	🍬	🍬	🍬	🍬	🍬	🍬	🍬	🍬	🍬	—	—	🍬	candy
710	U+1F36D	🍭	🍭	🍭	🍭	🍭	🍭	🍭	🍭	🍭	—	—	🍭	lollipop
711	U+1F36E	🍮	🍮	🍮	🍮	🍮	🍮	🍮	🍮	🍮	—	—	🍮	custard
712	U+1F36F	🍯	🍯	🍯	🍯	🍯	🍯	🍯	🍯	🍯	—	—	🍯	honey pot
drink
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
713	U+1F37C	🍼	🍼	🍼	🍼	🍼	🍼	🍼	🍼	—	—	—	—	baby bottle
714	U+1F95B	🥛	🥛	🥛	🥛	🥛	🥛	🥛	🥛	—	—	—	—	glass of milk
715	U+2615	☕	☕	☕	☕	☕	☕	☕	☕	☕	☕	☕	☕	hot beverage
716	U+1F375	🍵	🍵	🍵	🍵	🍵	🍵	🍵	🍵	🍵	🍵	🍵	🍵	teacup without handle
717	U+1F376	🍶	🍶	🍶	🍶	🍶	🍶	🍶	🍶	🍶	🍶	🍶	🍶	sake
718	U+1F37E	🍾	🍾	🍾	🍾	🍾	🍾	🍾	🍾	—	—	—	—	bottle with popping cork
719	U+1F377	🍷	🍷	🍷	🍷	🍷	🍷	🍷	🍷	🍷	—	🍷	🍷	wine glass
720	U+1F378	🍸	🍸	🍸	🍸	🍸	🍸	🍸	🍸	🍸	🍸	🍸	🍸	cocktail glass
721	U+1F379	🍹	🍹	🍹	🍹	🍹	🍹	🍹	🍹	🍹	—	—	🍹	tropical drink
722	U+1F37A	🍺	🍺	🍺	🍺	🍺	🍺	🍺	🍺	🍺	🍺	🍺	🍺	beer mug
723	U+1F37B	🍻	🍻	🍻	🍻	🍻	🍻	🍻	🍻	🍻	🍻	—	🍻	clinking beer mugs
724	U+1F942	🥂	🥂	🥂	🥂	🥂	🥂	🥂	🥂	—	—	—	—	clinking glasses
725	U+1F943	🥃	🥃	🥃	🥃	🥃	🥃	🥃	🥃	—	—	—	—	tumbler glass
726	U+1F964	🥤	🥤	🥤	🥤	🥤	🥤	🥤	🥤	—	—	—	—	cup with straw
727	U+1F9C3	🧃	🧃	🧃	🧃	🧃	🧃	🧃	🧃	—	—	—	—	beverage box
728	U+1F9C9	🧉	🧉	🧉	🧉	🧉	🧉	🧉	🧉	—	—	—	—	mate
729	U+1F9CA	🧊	🧊	🧊	🧊	🧊	🧊	🧊	🧊	—	—	—	—	ice
dishware
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
730	U+1F962	🥢	🥢	🥢	🥢	🥢	🥢	🥢	🥢	—	—	—	—	chopsticks
731	U+1F37D	🍽	🍽	🍽	🍽	🍽	🍽	🍽	🍽	—	—	—	—	fork and knife with plate
732	U+1F374	🍴	🍴	🍴	🍴	🍴	🍴	🍴	🍴	🍴	🍴	🍴	🍴	fork and knife
733	U+1F944	🥄	🥄	🥄	🥄	🥄	🥄	🥄	🥄	—	—	—	—	spoon
734	U+1F52A	🔪	🔪	🔪	🔪	🔪	🔪	🔪	🔪	🔪	—	—	🔪	kitchen knife
735	U+1F3FA	🏺	🏺	🏺	🏺	🏺	🏺	🏺	🏺	—	—	—	—	amphora
Travel & Places
place-map
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
736	U+1F30D	🌍	🌍	🌍	🌍	🌍	🌍	🌍	🌍	—	—	—	—	globe showing Europe-Africa
737	U+1F30E	🌎	🌎	🌎	🌎	🌎	🌎	🌎	🌎	—	—	—	—	globe showing Americas
738	U+1F30F	🌏	🌏	🌏	🌏	🌏	🌏	🌏	🌏	🌏	—	—	🌏	globe showing Asia-Australia
739	U+1F310	🌐	🌐	🌐	🌐	🌐	🌐	🌐	🌐	—	—	—	—	globe with meridians
740	U+1F5FA	🗺	🗺	🗺	🗺	🗺	🗺	🗺	🗺	—	—	—	—	world map
741	U+1F5FE	🗾	🗾	🗾	🗾	🗾	🗾	🗾	—	🗾	—	—	🗾	map of Japan
742	U+1F9ED	🧭	🧭	🧭	🧭	🧭	🧭	🧭	🧭	—	—	—	—	compass
place-geographic
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
743	U+1F3D4	🏔	🏔	🏔	🏔	🏔	🏔	🏔	🏔	—	—	—	—	snow-capped mountain
744	U+26F0	⛰	⛰	⛰	⛰	⛰	⛰	⛰	⛰	—	—	—	—	mountain
745	U+1F30B	🌋	🌋	🌋	🌋	🌋	🌋	🌋	🌋	🌋	—	—	🌋	volcano
746	U+1F5FB	🗻	🗻	🗻	🗻	🗻	🗻	🗻	🗻	🗻	🗻	🗻	🗻	mount fuji
747	U+1F3D5	🏕	🏕	🏕	🏕	🏕	🏕	🏕	🏕	—	—	—	—	camping
748	U+1F3D6	🏖	🏖	🏖	🏖	🏖	🏖	🏖	🏖	—	—	—	—	beach with umbrella
749	U+1F3DC	🏜	🏜	🏜	🏜	🏜	🏜	🏜	🏜	—	—	—	—	desert
750	U+1F3DD	🏝	🏝	🏝	🏝	🏝	🏝	🏝	🏝	—	—	—	—	desert island
751	U+1F3DE	🏞	🏞	🏞	🏞	🏞	🏞	🏞	🏞	—	—	—	—	national park
place-building
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
752	U+1F3DF	🏟	🏟	🏟	🏟	🏟	🏟	🏟	🏟	—	—	—	—	stadium
753	U+1F3DB	🏛	🏛	🏛	🏛	🏛	🏛	🏛	🏛	—	—	—	—	classical building
754	U+1F3D7	🏗	🏗	🏗	🏗	🏗	🏗	🏗	🏗	—	—	—	—	building construction
755	U+1F9F1	🧱	🧱	🧱	🧱	🧱	🧱	🧱	🧱	—	—	—	—	brick
756	U+1F3D8	🏘	🏘	🏘	🏘	🏘	🏘	🏘	🏘	—	—	—	—	houses
757	U+1F3DA	🏚	🏚	🏚	🏚	🏚	🏚	🏚	🏚	—	—	—	—	derelict house
758	U+1F3E0	🏠	🏠	🏠	🏠	🏠	🏠	🏠	🏠	🏠	🏠	🏠	🏠	house
759	U+1F3E1	🏡	🏡	🏡	🏡	🏡	🏡	🏡	🏡	🏡	—	—	🏡	house with garden
760	U+1F3E2	🏢	🏢	🏢	🏢	🏢	🏢	🏢	🏢	🏢	🏢	🏢	🏢	office building
761	U+1F3E3	🏣	🏣	🏣	🏣	🏣	🏣	🏣	🏣	🏣	🏣	🏣	🏣	Japanese post office
762	U+1F3E4	🏤	🏤	🏤	🏤	🏤	🏤	🏤	🏤	—	—	—	—	post office
763	U+1F3E5	🏥	🏥	🏥	🏥	🏥	🏥	🏥	🏥	🏥	🏥	🏥	🏥	hospital
764	U+1F3E6	🏦	🏦	🏦	🏦	🏦	🏦	🏦	🏦	🏦	🏦	🏦	🏦	bank
765	U+1F3E8	🏨	🏨	🏨	🏨	🏨	🏨	🏨	🏨	🏨	🏨	🏨	🏨	hotel
766	U+1F3E9	🏩	🏩	🏩	🏩	🏩	🏩	🏩	🏩	🏩	🏩	—	🏩	love hotel
767	U+1F3EA	🏪	🏪	🏪	🏪	🏪	🏪	🏪	🏪	🏪	🏪	🏪	🏪	convenience store
768	U+1F3EB	🏫	🏫	🏫	🏫	🏫	🏫	🏫	🏫	🏫	🏫	🏫	🏫	school
769	U+1F3EC	🏬	🏬	🏬	🏬	🏬	🏬	🏬	🏬	🏬	🏬	—	🏬	department store
770	U+1F3ED	🏭	🏭	🏭	🏭	🏭	🏭	🏭	🏭	🏭	🏭	—	🏭	factory
771	U+1F3EF	🏯	🏯	🏯	🏯	🏯	🏯	🏯	🏯	🏯	🏯	—	🏯	Japanese castle
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
772	U+1F3F0	🏰	🏰	🏰	🏰	🏰	🏰	🏰	🏰	🏰	🏰	—	🏰	castle
773	U+1F492	💒	💒	💒	💒	💒	💒	💒	💒	💒	💒	—	—	wedding
774	U+1F5FC	🗼	🗼	🗼	🗼	🗼	🗼	🗼	🗼	🗼	🗼	—	🗼	Tokyo tower
775	U+1F5FD	🗽	🗽	🗽	🗽	🗽	🗽	🗽	🗽	🗽	🗽	—	—	Statue of Liberty
place-religious
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
776	U+26EA	⛪	⛪	⛪	⛪	⛪	⛪	⛪	⛪	⛪	⛪	—	⛪	church
777	U+1F54C	🕌	🕌	🕌	🕌	🕌	🕌	🕌	🕌	—	—	—	—	mosque
778	U+1F6D5	🛕	🛕	🛕	🛕	🛕	🛕	🛕	🛕	—	—	—	—	hindu temple
779	U+1F54D	🕍	🕍	🕍	🕍	🕍	🕍	🕍	🕍	—	—	—	—	synagogue
780	U+26E9	⛩	⛩	⛩	⛩	⛩	⛩	⛩	⛩	—	—	—	—	shinto shrine
781	U+1F54B	🕋	🕋	🕋	🕋	🕋	🕋	🕋	🕋	—	—	—	—	kaaba
place-other
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
782	U+26F2	⛲	⛲	⛲	⛲	⛲	⛲	⛲	⛲	⛲	⛲	—	⛲	fountain
783	U+26FA	⛺	⛺	⛺	⛺	⛺	⛺	⛺	⛺	⛺	⛺	—	⛺	tent
784	U+1F301	🌁	🌁	🌁	🌁	🌁	🌁	🌁	🌁	🌁	—	🌁	🌁	foggy
785	U+1F303	🌃	🌃	🌃	🌃	🌃	🌃	🌃	🌃	🌃	🌃	🌃	🌃	night with stars
786	U+1F3D9	🏙	🏙	🏙	🏙	🏙	🏙	🏙	🏙	—	—	—	—	cityscape
787	U+1F304	🌄	🌄	🌄	🌄	🌄	🌄	🌄	🌄	🌄	🌄	—	—	sunrise over mountains
788	U+1F305	🌅	🌅	🌅	🌅	🌅	🌅	🌅	🌅	🌅	🌅	—	🌅	sunrise
789	U+1F306	🌆	🌆	🌆	🌆	🌆	🌆	🌆	🌆	🌆	🌆	—	🌆	cityscape at dusk
790	U+1F307	🌇	🌇	🌇	🌇	🌇	🌇	🌇	🌇	🌇	🌇	—	—	sunset
791	U+1F309	🌉	🌉	🌉	🌉	🌉	🌉	🌉	🌉	🌉	—	—	🌉	bridge at night
792	U+2668	♨	♨	♨	♨	♨	♨	♨	♨	♨	♨	♨	♨	hot springs
793	U+1F3A0	🎠	🎠	🎠	🎠	🎠	🎠	🎠	🎠	🎠	—	🎠	—	carousel horse
794	U+1F3A1	🎡	🎡	🎡	🎡	🎡	🎡	🎡	🎡	🎡	🎡	—	🎡	ferris wheel
795	U+1F3A2	🎢	🎢	🎢	🎢	🎢	🎢	🎢	🎢	🎢	🎢	—	🎢	roller coaster
796	U+1F488	💈	💈	💈	💈	💈	💈	💈	💈	💈	💈	—	💈	barber pole
797	U+1F3AA	🎪	🎪	🎪	🎪	🎪	🎪	🎪	🎪	🎪	—	🎪	🎪	circus tent
transport-ground
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
798	U+1F682	🚂	🚂	🚂	🚂	🚂	🚂	🚂	🚂	—	—	—	—	locomotive
799	U+1F683	🚃	🚃	🚃	🚃	🚃	🚃	🚃	🚃	🚃	🚃	🚃	🚃	railway car
800	U+1F684	🚄	🚄	🚄	🚄	🚄	🚄	🚄	🚄	🚄	🚄	🚄	—	high-speed train
801	U+1F685	🚅	🚅	🚅	🚅	🚅	🚅	🚅	🚅	🚅	🚅	—	🚅	bullet train
802	U+1F686	🚆	🚆	🚆	🚆	🚆	🚆	🚆	🚆	—	—	—	—	train
803	U+1F687	🚇	🚇	🚇	🚇	🚇	🚇	🚇	🚇	🚇	🚇	—	🚇	metro
804	U+1F688	🚈	🚈	🚈	🚈	🚈	🚈	🚈	🚈	—	—	—	—	light rail
805	U+1F689	🚉	🚉	🚉	🚉	🚉	🚉	🚉	🚉	🚉	🚉	—	🚉	station
806	U+1F68A	🚊	🚊	🚊	🚊	🚊	🚊	🚊	🚊	—	—	—	—	tram
807	U+1F69D	🚝	🚝	🚝	🚝	🚝	🚝	🚝	🚝	—	—	—	—	monorail
808	U+1F69E	🚞	🚞	🚞	🚞	🚞	🚞	🚞	🚞	—	—	—	—	mountain railway
809	U+1F68B	🚋	🚋	🚋	🚋	🚋	🚋	🚋	🚋	—	—	—	—	tram car
810	U+1F68C	🚌	🚌	🚌	🚌	🚌	🚌	🚌	🚌	🚌	🚌	🚌	🚌	bus
811	U+1F68D	🚍	🚍	🚍	🚍	🚍	🚍	🚍	🚍	—	—	—	—	oncoming bus
812	U+1F68E	🚎	🚎	🚎	🚎	🚎	🚎	🚎	🚎	—	—	—	—	trolleybus
813	U+1F690	🚐	🚐	🚐	🚐	🚐	🚐	🚐	🚐	—	—	—	—	minibus
814	U+1F691	🚑	🚑	🚑	🚑	🚑	🚑	🚑	🚑	🚑	🚑	—	🚑	ambulance
815	U+1F692	🚒	🚒	🚒	🚒	🚒	🚒	🚒	🚒	🚒	🚒	—	🚒	fire engine
816	U+1F693	🚓	🚓	🚓	🚓	🚓	🚓	🚓	🚓	🚓	🚓	—	🚓	police car
817	U+1F694	🚔	🚔	🚔	🚔	🚔	🚔	🚔	🚔	—	—	—	—	oncoming police car
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
818	U+1F695	🚕	🚕	🚕	🚕	🚕	🚕	🚕	🚕	🚕	🚕	—	—	taxi
819	U+1F696	🚖	🚖	🚖	🚖	🚖	🚖	🚖	🚖	—	—	—	—	oncoming taxi
820	U+1F697	🚗	🚗	🚗	🚗	🚗	🚗	🚗	🚗	🚗	🚗	🚗	🚗	automobile
821	U+1F698	🚘	🚘	🚘	🚘	🚘	🚘	🚘	🚘	—	—	—	—	oncoming automobile
822	U+1F699	🚙	🚙	🚙	🚙	🚙	🚙	🚙	🚙	🚙	🚙	🚙	—	sport utility vehicle
823	U+1F69A	🚚	🚚	🚚	🚚	🚚	🚚	🚚	🚚	🚚	🚚	—	🚚	delivery truck
824	U+1F69B	🚛	🚛	🚛	🚛	🚛	🚛	🚛	🚛	—	—	—	—	articulated lorry
825	U+1F69C	🚜	🚜	🚜	🚜	🚜	🚜	🚜	🚜	—	—	—	—	tractor
826	U+1F3CE	🏎	🏎	🏎	🏎	🏎	🏎	🏎	🏎	—	—	—	—	racing car
827	U+1F3CD	🏍	🏍	🏍	🏍	🏍	🏍	🏍	🏍	—	—	—	—	motorcycle
828	U+1F6F5	🛵	🛵	🛵	🛵	🛵	🛵	🛵	🛵	—	—	—	—	motor scooter
829	U+1F9BD	🦽	🦽	🦽	🦽	🦽	🦽	🦽	🦽	—	—	—	—	manual wheelchair
830	U+1F9BC	🦼	🦼	🦼	🦼	🦼	🦼	🦼	🦼	—	—	—	—	motorized wheelchair
831	U+1F6FA	🛺	🛺	🛺	🛺	🛺	🛺	🛺	🛺	—	—	—	—	auto rickshaw
832	U+1F6B2	🚲	🚲	🚲	🚲	🚲	🚲	🚲	🚲	🚲	🚲	🚲	🚲	bicycle
833	U+1F6F4	🛴	🛴	🛴	🛴	🛴	🛴	🛴	🛴	—	—	—	—	kick scooter
834	U+1F6F9	🛹	🛹	🛹	🛹	🛹	🛹	🛹	🛹	—	—	—	—	skateboard
835	U+1F68F	🚏	🚏	🚏	🚏	🚏	🚏	🚏	🚏	🚏	🚏	—	🚏	bus stop
836	U+1F6E3	🛣	🛣	🛣	🛣	🛣	🛣	🛣	🛣	—	—	—	—	motorway
837	U+1F6E4	🛤	🛤	🛤	🛤	🛤	🛤	🛤	🛤	—	—	—	—	railway track
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
838	U+1F6E2	🛢	🛢	🛢	🛢	🛢	🛢	🛢	🛢	—	—	—	—	oil drum
839	U+26FD	⛽	⛽	⛽	⛽	⛽	⛽	⛽	⛽	⛽	⛽	⛽	⛽	fuel pump
840	U+1F6A8	🚨	🚨	🚨	🚨	🚨	🚨	🚨	🚨	🚨	—	—	🚨	police car light
841	U+1F6A5	🚥	🚥	🚥	🚥	🚥	🚥	🚥	🚥	🚥	🚥	🚥	🚥	horizontal traffic light
842	U+1F6A6	🚦	🚦	🚦	🚦	🚦	🚦	🚦	🚦	—	—	—	—	vertical traffic light
843	U+1F6D1	🛑	🛑	🛑	🛑	🛑	🛑	🛑	🛑	—	—	—	—	stop sign
844	U+1F6A7	🚧	🚧	🚧	🚧	🚧	🚧	🚧	🚧	🚧	🚧	—	🚧	construction
transport-water
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
845	U+2693	⚓	⚓	⚓	⚓	⚓	⚓	⚓	⚓	⚓	—	—	⚓	anchor
846	U+26F5	⛵	⛵	⛵	⛵	⛵	⛵	⛵	⛵	⛵	⛵	⛵	⛵	sailboat
847	U+1F6F6	🛶	🛶	🛶	🛶	🛶	🛶	🛶	🛶	—	—	—	—	canoe
848	U+1F6A4	🚤	🚤	🚤	🚤	🚤	🚤	🚤	🚤	🚤	🚤	—	—	speedboat
849	U+1F6F3	🛳	🛳	🛳	🛳	🛳	🛳	🛳	🛳	—	—	—	—	passenger ship
850	U+26F4	⛴	⛴	⛴	⛴	⛴	⛴	⛴	⛴	—	—	—	—	ferry
851	U+1F6E5	🛥	🛥	🛥	🛥	🛥	🛥	🛥	🛥	—	—	—	—	motor boat
852	U+1F6A2	🚢	🚢	🚢	🚢	🚢	🚢	🚢	🚢	🚢	🚢	🚢	🚢	ship
transport-air
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
853	U+2708	✈	✈	✈	✈	✈	✈	✈	✈	✈	✈	✈	✈	airplane
854	U+1F6E9	🛩	🛩	🛩	🛩	🛩	🛩	🛩	🛩	—	—	—	—	small airplane
855	U+1F6EB	🛫	🛫	🛫	🛫	🛫	🛫	🛫	🛫	—	—	—	—	airplane departure
856	U+1F6EC	🛬	🛬	🛬	🛬	🛬	🛬	🛬	🛬	—	—	—	—	airplane arrival
857	U+1FA82	🪂	🪂	🪂	🪂	🪂	🪂	🪂	🪂	—	—	—	—	parachute
858	U+1F4BA	💺	💺	💺	💺	💺	💺	💺	💺	💺	💺	💺	—	seat
859	U+1F681	🚁	🚁	🚁	🚁	🚁	🚁	🚁	🚁	—	—	—	—	helicopter
860	U+1F69F	🚟	🚟	🚟	🚟	🚟	🚟	🚟	🚟	—	—	—	—	suspension railway
861	U+1F6A0	🚠	🚠	🚠	🚠	🚠	🚠	🚠	🚠	—	—	—	—	mountain cableway
862	U+1F6A1	🚡	🚡	🚡	🚡	🚡	🚡	🚡	🚡	—	—	—	—	aerial tramway
863	U+1F6F0	🛰	🛰	🛰	🛰	🛰	🛰	🛰	🛰	—	—	—	—	satellite
864	U+1F680	🚀	🚀	🚀	🚀	🚀	🚀	🚀	🚀	🚀	🚀	—	🚀	rocket
865	U+1F6F8	🛸	🛸	🛸	🛸	🛸	🛸	🛸	🛸	—	—	—	—	flying saucer
hotel
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
866	U+1F6CE	🛎	🛎	🛎	🛎	🛎	🛎	🛎	🛎	—	—	—	—	bellhop bell
867	U+1F9F3	🧳	🧳	🧳	🧳	🧳	🧳	🧳	🧳	—	—	—	—	luggage
time
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
868	U+231B	⌛	⌛	⌛	⌛	⌛	⌛	⌛	⌛	⌛	—	—	⌛	hourglass done
869	U+23F3	⏳	⏳	⏳	⏳	⏳	⏳	⏳	⏳	⏳	—	⏳	⏳	hourglass not done
870	U+231A	⌚	⌚	⌚	⌚	⌚	⌚	⌚	⌚	⌚	—	⌚	⌚	watch
871	U+23F0	⏰	⏰	⏰	⏰	⏰	⏰	⏰	⏰	⏰	—	⏰	⏰	alarm clock
872	U+23F1	⏱	⏱	⏱	⏱	⏱	⏱	⏱	⏱	—	—	—	—	stopwatch
873	U+23F2	⏲	⏲	⏲	⏲	⏲	⏲	⏲	⏲	—	—	—	—	timer clock
874	U+1F570	🕰	🕰	🕰	🕰	🕰	🕰	🕰	🕰	—	—	—	—	mantelpiece clock
875	U+1F55B	🕛	🕛	🕛	🕛	🕛	🕛	🕛	🕛	🕛	🕛	—	—	twelve o’clock
876	U+1F567	🕧	🕧	🕧	🕧	🕧	🕧	🕧	🕧	—	—	—	—	twelve-thirty
877	U+1F550	🕐	🕐	🕐	🕐	🕐	🕐	🕐	🕐	🕐	🕐	—	—	one o’clock
878	U+1F55C	🕜	🕜	🕜	🕜	🕜	🕜	🕜	🕜	—	—	—	—	one-thirty
879	U+1F551	🕑	🕑	🕑	🕑	🕑	🕑	🕑	🕑	🕑	🕑	—	—	two o’clock
880	U+1F55D	🕝	🕝	🕝	🕝	🕝	🕝	🕝	🕝	—	—	—	—	two-thirty
881	U+1F552	🕒	🕒	🕒	🕒	🕒	🕒	🕒	🕒	🕒	🕒	—	—	three o’clock
882	U+1F55E	🕞	🕞	🕞	🕞	🕞	🕞	🕞	🕞	—	—	—	—	three-thirty
883	U+1F553	🕓	🕓	🕓	🕓	🕓	🕓	🕓	🕓	🕓	🕓	—	—	four o’clock
884	U+1F55F	🕟	🕟	🕟	🕟	🕟	🕟	🕟	🕟	—	—	—	—	four-thirty
885	U+1F554	🕔	🕔	🕔	🕔	🕔	🕔	🕔	🕔	🕔	🕔	—	—	five o’clock
886	U+1F560	🕠	🕠	🕠	🕠	🕠	🕠	🕠	🕠	—	—	—	—	five-thirty
887	U+1F555	🕕	🕕	🕕	🕕	🕕	🕕	🕕	🕕	🕕	🕕	—	—	six o’clock
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
888	U+1F561	🕡	🕡	🕡	🕡	🕡	🕡	🕡	🕡	—	—	—	—	six-thirty
889	U+1F556	🕖	🕖	🕖	🕖	🕖	🕖	🕖	🕖	🕖	🕖	—	—	seven o’clock
890	U+1F562	🕢	🕢	🕢	🕢	🕢	🕢	🕢	🕢	—	—	—	—	seven-thirty
891	U+1F557	🕗	🕗	🕗	🕗	🕗	🕗	🕗	🕗	🕗	🕗	—	—	eight o’clock
892	U+1F563	🕣	🕣	🕣	🕣	🕣	🕣	🕣	🕣	—	—	—	—	eight-thirty
893	U+1F558	🕘	🕘	🕘	🕘	🕘	🕘	🕘	🕘	🕘	🕘	—	—	nine o’clock
894	U+1F564	🕤	🕤	🕤	🕤	🕤	🕤	🕤	🕤	—	—	—	—	nine-thirty
895	U+1F559	🕙	🕙	🕙	🕙	🕙	🕙	🕙	🕙	🕙	🕙	—	—	ten o’clock
896	U+1F565	🕥	🕥	🕥	🕥	🕥	🕥	🕥	🕥	—	—	—	—	ten-thirty
897	U+1F55A	🕚	🕚	🕚	🕚	🕚	🕚	🕚	🕚	🕚	🕚	—	—	eleven o’clock
898	U+1F566	🕦	🕦	🕦	🕦	🕦	🕦	🕦	🕦	—	—	—	—	eleven-thirty
sky & weather
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
899	U+1F311	🌑	🌑	🌑	🌑	🌑	🌑	🌑	🌑	🌑	—	🌑	🌑	new moon
900	U+1F312	🌒	🌒	🌒	🌒	🌒	🌒	🌒	🌒	—	—	—	—	waxing crescent moon
901	U+1F313	🌓	🌓	🌓	🌓	🌓	🌓	🌓	🌓	🌓	—	🌓	🌓	first quarter moon
902	U+1F314	🌔	🌔	🌔	🌔	🌔	🌔	🌔	🌔	🌔	—	🌔	🌔	waxing gibbous moon
903	U+1F315	🌕	🌕	🌕	🌕	🌕	🌕	🌕	🌕	🌕	—	🌕	—	full moon
904	U+1F316	🌖	🌖	🌖	🌖	🌖	🌖	🌖	🌖	—	—	—	—	waning gibbous moon
905	U+1F317	🌗	🌗	🌗	🌗	🌗	🌗	🌗	🌗	—	—	—	—	last quarter moon
906	U+1F318	🌘	🌘	🌘	🌘	🌘	🌘	🌘	🌘	—	—	—	—	waning crescent moon
907	U+1F319	🌙	🌙	🌙	🌙	🌙	🌙	🌙	🌙	🌙	🌙	🌙	🌙	crescent moon
908	U+1F31A	🌚	🌚	🌚	🌚	🌚	🌚	🌚	🌚	—	—	—	—	new moon face
909	U+1F31B	🌛	🌛	🌛	🌛	🌛	🌛	🌛	🌛	🌛	—	—	🌛	first quarter moon face
910	U+1F31C	🌜	🌜	🌜	🌜	🌜	🌜	🌜	🌜	—	—	—	—	last quarter moon face
911	U+1F321	🌡	🌡	🌡	🌡	🌡	🌡	🌡	🌡	—	—	—	—	thermometer
912	U+2600	☀	☀	☀	☀	☀	☀	☀	☀	☀	☀	☀	☀	sun
913	U+1F31D	🌝	🌝	🌝	🌝	🌝	🌝	🌝	🌝	—	—	—	—	full moon face
914	U+1F31E	🌞	🌞	🌞	🌞	🌞	🌞	🌞	🌞	—	—	—	—	sun with face
915	U+1FA90	🪐	🪐	🪐	🪐	🪐	🪐	🪐	🪐	—	—	—	—	ringed planet
916	U+2B50	⭐	⭐	⭐	⭐	⭐	⭐	⭐	⭐	—	⭐	—	⭐	star
917	U+1F31F	🌟	🌟	🌟	🌟	🌟	🌟	🌟	🌟	🌟	🌟	—	—	glowing star
918	U+1F320	🌠	🌠	🌠	🌠	🌠	🌠	🌠	🌠	🌠	—	—	🌠	shooting star
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
919	U+1F30C	🌌	🌌	🌌	🌌	🌌	🌌	🌌	🌌	🌌	—	—	🌌	milky way
920	U+2601	☁	☁	☁	☁	☁	☁	☁	☁	☁	☁	☁	☁	cloud
921	U+26C5	⛅	⛅	⛅	⛅	⛅	⛅	⛅	⛅	⛅	—	—	⛅	sun behind cloud
922	U+26C8	⛈	⛈	⛈	⛈	⛈	⛈	⛈	⛈	—	—	—	—	cloud with lightning and rain
923	U+1F324	🌤	🌤	🌤	🌤	🌤	🌤	🌤	🌤	—	—	—	—	sun behind small cloud
924	U+1F325	🌥	🌥	🌥	🌥	🌥	🌥	🌥	🌥	—	—	—	—	sun behind large cloud
925	U+1F326	🌦	🌦	🌦	🌦	🌦	🌦	🌦	🌦	—	—	—	—	sun behind rain cloud
926	U+1F327	🌧	🌧	🌧	🌧	🌧	🌧	🌧	🌧	—	—	—	—	cloud with rain
927	U+1F328	🌨	🌨	🌨	🌨	🌨	🌨	🌨	🌨	—	—	—	—	cloud with snow
928	U+1F329	🌩	🌩	🌩	🌩	🌩	🌩	🌩	🌩	—	—	—	—	cloud with lightning
929	U+1F32A	🌪	🌪	🌪	🌪	🌪	🌪	🌪	🌪	—	—	—	—	tornado
930	U+1F32B	🌫	🌫	🌫	🌫	🌫	🌫	🌫	🌫	—	—	—	—	fog
931	U+1F32C	🌬	🌬	🌬	🌬	🌬	🌬	🌬	🌬	—	—	—	—	wind face
932	U+1F300	🌀	🌀	🌀	🌀	🌀	🌀	🌀	🌀	🌀	🌀	🌀	🌀	cyclone
933	U+1F308	🌈	🌈	🌈	🌈	🌈	🌈	🌈	🌈	🌈	🌈	—	🌈	rainbow
934	U+1F302	🌂	🌂	🌂	🌂	🌂	🌂	🌂	🌂	🌂	🌂	🌂	🌂	closed umbrella
935	U+2602	☂	☂	☂	☂	☂	☂	☂	☂	—	—	—	—	umbrella
936	U+2614	☔	☔	☔	☔	☔	☔	☔	☔	☔	☔	☔	☔	umbrella with rain drops
937	U+26F1	⛱	⛱	⛱	⛱	⛱	⛱	⛱	⛱	—	—	—	—	umbrella on ground
938	U+26A1	⚡	⚡	⚡	⚡	⚡	⚡	⚡	⚡	⚡	⚡	⚡	⚡	high voltage
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
939	U+2744	❄	❄	❄	❄	❄	❄	❄	❄	❄	—	—	❄	snowflake
940	U+2603	☃	☃	☃	☃	☃	☃	☃	☃	—	—	—	—	snowman
941	U+26C4	⛄	⛄	⛄	⛄	⛄	⛄	⛄	⛄	⛄	⛄	⛄	⛄	snowman without snow
942	U+2604	☄	☄	☄	☄	☄	☄	☄	☄	—	—	—	—	comet
943	U+1F525	🔥	🔥	🔥	🔥	🔥	🔥	🔥	🔥	🔥	🔥	—	🔥	fire
944	U+1F4A7	💧	💧	💧	💧	💧	💧	💧	💧	💧	—	💧	💧	droplet
945	U+1F30A	🌊	🌊	🌊	🌊	🌊	🌊	🌊	🌊	🌊	🌊	🌊	🌊	water wave
Activities
event
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
946	U+1F383	🎃	🎃	🎃	🎃	🎃	🎃	🎃	🎃	🎃	🎃	—	🎃	jack-o-lantern
947	U+1F384	🎄	🎄	🎄	🎄	🎄	🎄	🎄	🎄	🎄	🎄	🎄	🎄	Christmas tree
948	U+1F386	🎆	🎆	🎆	🎆	🎆	🎆	🎆	🎆	🎆	🎆	—	🎆	fireworks
949	U+1F387	🎇	🎇	🎇	🎇	🎇	🎇	🎇	🎇	🎇	🎇	—	🎇	sparkler
950	U+1F9E8	🧨	🧨	🧨	🧨	🧨	🧨	🧨	🧨	—	—	—	—	firecracker
951	U+2728	✨	✨	✨	✨	✨	✨	✨	✨	✨	✨	✨	✨	sparkles
952	U+1F388	🎈	🎈	🎈	🎈	🎈	🎈	🎈	🎈	🎈	🎈	—	🎈	balloon
953	U+1F389	🎉	🎉	🎉	🎉	🎉	🎉	🎉	🎉	🎉	🎉	—	🎉	party popper
954	U+1F38A	🎊	🎊	🎊	🎊	🎊	🎊	🎊	🎊	🎊	—	—	🎊	confetti ball
955	U+1F38B	🎋	🎋	🎋	🎋	🎋	🎋	🎋	🎋	🎋	—	—	🎋	tanabata tree
956	U+1F38D	🎍	🎍	🎍	🎍	🎍	🎍	🎍	🎍	🎍	🎍	—	🎍	pine decoration
957	U+1F38E	🎎	🎎	🎎	🎎	🎎	🎎	🎎	🎎	🎎	🎎	—	🎎	Japanese dolls
958	U+1F38F	🎏	🎏	🎏	🎏	🎏	🎏	🎏	🎏	🎏	🎏	—	🎏	carp streamer
959	U+1F390	🎐	🎐	🎐	🎐	🎐	🎐	🎐	🎐	🎐	🎐	—	🎐	wind chime
960	U+1F391	🎑	🎑	🎑	🎑	🎑	🎑	🎑	🎑	🎑	🎑	—	🎑	moon viewing ceremony
961	U+1F9E7	🧧	🧧	🧧	🧧	🧧	🧧	🧧	🧧	—	—	—	—	red envelope
962	U+1F380	🎀	🎀	🎀	🎀	🎀	🎀	🎀	🎀	🎀	🎀	🎀	🎀	ribbon
963	U+1F381	🎁	🎁	🎁	🎁	🎁	🎁	🎁	🎁	🎁	🎁	🎁	🎁	wrapped gift
964	U+1F397	🎗	🎗	🎗	🎗	🎗	🎗	🎗	🎗	—	—	—	—	reminder ribbon
965	U+1F39F	🎟	🎟	🎟	🎟	🎟	🎟	🎟	🎟	—	—	—	—	admission tickets
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
966	U+1F3AB	🎫	🎫	🎫	🎫	🎫	🎫	🎫	🎫	🎫	🎫	🎫	🎫	ticket
award-medal
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
967	U+1F396	🎖	🎖	🎖	🎖	🎖	🎖	🎖	🎖	—	—	—	—	military medal
968	U+1F3C6	🏆	🏆	🏆	🏆	🏆	🏆	🏆	🏆	🏆	🏆	—	🏆	trophy
969	U+1F3C5	🏅	🏅	🏅	🏅	🏅	🏅	🏅	🏅	—	—	—	—	sports medal
970	U+1F947	🥇	🥇	🥇	🥇	🥇	🥇	🥇	🥇	—	—	—	—	1st place medal
971	U+1F948	🥈	🥈	🥈	🥈	🥈	🥈	🥈	🥈	—	—	—	—	2nd place medal
972	U+1F949	🥉	🥉	🥉	🥉	🥉	🥉	🥉	🥉	—	—	—	—	3rd place medal
sport
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
973	U+26BD	⚽	⚽	⚽	⚽	⚽	⚽	⚽	⚽	⚽	⚽	⚽	⚽	soccer ball
974	U+26BE	⚾	⚾	⚾	⚾	⚾	⚾	⚾	⚾	⚾	⚾	⚾	⚾	baseball
975	U+1F94E	🥎	🥎	🥎	🥎	🥎	🥎	🥎	🥎	—	—	—	—	softball
976	U+1F3C0	🏀	🏀	🏀	🏀	🏀	🏀	🏀	🏀	🏀	🏀	🏀	🏀	basketball
977	U+1F3D0	🏐	🏐	🏐	🏐	🏐	🏐	🏐	🏐	—	—	—	—	volleyball
978	U+1F3C8	🏈	🏈	🏈	🏈	🏈	🏈	🏈	🏈	🏈	🏈	—	🏈	american football
979	U+1F3C9	🏉	🏉	🏉	🏉	🏉	🏉	🏉	🏉	—	—	—	—	rugby football
980	U+1F3BE	🎾	🎾	🎾	🎾	🎾	🎾	🎾	🎾	🎾	🎾	🎾	🎾	tennis
981	U+1F94F	🥏	🥏	🥏	🥏	🥏	🥏	🥏	🥏	—	—	—	—	flying disc
982	U+1F3B3	🎳	🎳	🎳	🎳	🎳	🎳	🎳	🎳	🎳	—	—	🎳	bowling
983	U+1F3CF	🏏	🏏	🏏	🏏	🏏	🏏	🏏	🏏	—	—	—	—	cricket game
984	U+1F3D1	🏑	🏑	🏑	🏑	🏑	🏑	🏑	🏑	—	—	—	—	field hockey
985	U+1F3D2	🏒	🏒	🏒	🏒	🏒	🏒	🏒	🏒	—	—	—	—	ice hockey
986	U+1F94D	🥍	🥍	🥍	🥍	🥍	🥍	🥍	🥍	—	—	—	—	lacrosse
987	U+1F3D3	🏓	🏓	🏓	🏓	🏓	🏓	🏓	🏓	—	—	—	—	ping pong
988	U+1F3F8	🏸	🏸	🏸	🏸	🏸	🏸	🏸	🏸	—	—	—	—	badminton
989	U+1F94A	🥊	🥊	🥊	🥊	🥊	🥊	🥊	🥊	—	—	—	—	boxing glove
990	U+1F94B	🥋	🥋	🥋	🥋	🥋	🥋	🥋	🥋	—	—	—	—	martial arts uniform
991	U+1F945	🥅	🥅	🥅	🥅	🥅	🥅	🥅	🥅	—	—	—	—	goal net
992	U+26F3	⛳	⛳	⛳	⛳	⛳	⛳	⛳	⛳	⛳	⛳	⛳	⛳	flag in hole
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
993	U+26F8	⛸	⛸	⛸	⛸	⛸	⛸	⛸	⛸	—	—	—	—	ice skate
994	U+1F3A3	🎣	🎣	🎣	🎣	🎣	🎣	🎣	🎣	🎣	—	—	🎣	fishing pole
995	U+1F93F	🤿	🤿	🤿	🤿	🤿	🤿	🤿	🤿	—	—	—	—	diving mask
996	U+1F3BD	🎽	🎽	🎽	🎽	🎽	🎽	🎽	🎽	🎽	—	🎽	—	running shirt
997	U+1F3BF	🎿	🎿	🎿	🎿	🎿	🎿	🎿	🎿	🎿	🎿	🎿	🎿	skis
998	U+1F6F7	🛷	🛷	🛷	🛷	🛷	🛷	🛷	🛷	—	—	—	—	sled
999	U+1F94C	🥌	🥌	🥌	🥌	🥌	🥌	🥌	🥌	—	—	—	—	curling stone
game
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1000	U+1F3AF	🎯	🎯	🎯	🎯	🎯	🎯	🎯	🎯	🎯	🎯	—	🎯	direct hit
1001	U+1FA80	🪀	🪀	🪀	🪀	🪀	🪀	🪀	🪀	—	—	—	—	yo-yo
1002	U+1FA81	🪁	🪁	🪁	🪁	🪁	🪁	🪁	🪁	—	—	—	—	kite
1003	U+1F3B1	🎱	🎱	🎱	🎱	🎱	🎱	🎱	🎱	🎱	🎱	—	🎱	pool 8 ball
1004	U+1F52E	🔮	🔮	🔮	🔮	🔮	🔮	🔮	🔮	🔮	—	—	🔮	crystal ball
1005	U+1F9FF	🧿	🧿	🧿	🧿	🧿	🧿	🧿	🧿	—	—	—	—	nazar amulet
1006	U+1F3AE	🎮	🎮	🎮	🎮	🎮	🎮	🎮	🎮	🎮	—	🎮	🎮	video game
1007	U+1F579	🕹	🕹	🕹	🕹	🕹	🕹	🕹	🕹	—	—	—	—	joystick
1008	U+1F3B0	🎰	🎰	🎰	🎰	🎰	🎰	🎰	🎰	🎰	🎰	—	🎰	slot machine
1009	U+1F3B2	🎲	🎲	🎲	🎲	🎲	🎲	🎲	🎲	🎲	—	—	🎲	game die
1010	U+1F9E9	🧩	🧩	🧩	🧩	🧩	🧩	🧩	🧩	—	—	—	—	puzzle piece
1011	U+1F9F8	🧸	🧸	🧸	🧸	🧸	🧸	🧸	🧸	—	—	—	—	teddy bear
1012	U+2660	♠	♠	♠	♠	♠	♠	♠	♠	♠	♠	♠	♠	spade suit
1013	U+2665	♥	♥	♥	♥	♥	♥	♥	♥	♥	♥	♥	♥	heart suit
1014	U+2666	♦	♦	♦	♦	♦	♦	♦	♦	♦	♦	♦	♦	diamond suit
1015	U+2663	♣	♣	♣	♣	♣	♣	♣	♣	♣	♣	♣	♣	club suit
1016	U+265F	♟	♟	♟	♟	♟	♟	♟	♟	—	—	—	—	chess pawn
1017	U+1F0CF	🃏	🃏	🃏	🃏	🃏	🃏	🃏	🃏	🃏	—	—	🃏	joker
1018	U+1F004	🀄	🀄	🀄	🀄	🀄	🀄	🀄	🀄	🀄	🀄	—	🀄	mahjong red dragon
1019	U+1F3B4	🎴	🎴	🎴	🎴	🎴	🎴	🎴	🎴	🎴	—	—	🎴	flower playing cards
arts & crafts
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1020	U+1F3AD	🎭	🎭	🎭	🎭	🎭	🎭	🎭	🎭	🎭	—	—	🎭	performing arts
1021	U+1F5BC	🖼	🖼	🖼	🖼	🖼	🖼	🖼	🖼	—	—	—	—	framed picture
1022	U+1F3A8	🎨	🎨	🎨	🎨	🎨	🎨	🎨	🎨	🎨	🎨	🎨	🎨	artist palette
1023	U+1F9F5	🧵	🧵	🧵	🧵	🧵	🧵	🧵	🧵	—	—	—	—	thread
1024	U+1F9F6	🧶	🧶	🧶	🧶	🧶	🧶	🧶	🧶	—	—	—	—	yarn
Objects
clothing
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1025	U+1F453	👓	👓	👓	👓	👓	👓	👓	👓	👓	—	👓	👓	glasses
1026	U+1F576	🕶	🕶	🕶	🕶	🕶	🕶	🕶	🕶	—	—	—	—	sunglasses
1027	U+1F97D	🥽	🥽	🥽	🥽	🥽	🥽	🥽	🥽	—	—	—	—	goggles
1028	U+1F97C	🥼	🥼	🥼	🥼	🥼	🥼	🥼	🥼	—	—	—	—	lab coat
1029	U+1F9BA	🦺	🦺	🦺	🦺	🦺	🦺	🦺	🦺	—	—	—	—	safety vest
1030	U+1F454	👔	👔	👔	👔	👔	👔	👔	👔	👔	👔	—	👔	necktie
1031	U+1F455	👕	👕	👕	👕	👕	👕	👕	👕	👕	👕	👕	👕	t-shirt
1032	U+1F456	👖	👖	👖	👖	👖	👖	👖	👖	👖	—	👖	👖	jeans
1033	U+1F9E3	🧣	🧣	🧣	🧣	🧣	🧣	🧣	🧣	—	—	—	—	scarf
1034	U+1F9E4	🧤	🧤	🧤	🧤	🧤	🧤	🧤	🧤	—	—	—	—	gloves
1035	U+1F9E5	🧥	🧥	🧥	🧥	🧥	🧥	🧥	🧥	—	—	—	—	coat
1036	U+1F9E6	🧦	🧦	🧦	🧦	🧦	🧦	🧦	🧦	—	—	—	—	socks
1037	U+1F457	👗	👗	👗	👗	👗	👗	👗	👗	👗	👗	—	👗	dress
1038	U+1F458	👘	👘	👘	👘	👘	👘	👘	👘	👘	👘	—	👘	kimono
1039	U+1F97B	🥻	🥻	🥻	🥻	🥻	🥻	🥻	🥻	—	—	—	—	sari
1040	U+1FA71	🩱	🩱	🩱	🩱	🩱	🩱	🩱	🩱	—	—	—	—	one-piece swimsuit
1041	U+1FA72	🩲	🩲	🩲	🩲	🩲	🩲	🩲	🩲	—	—	—	—	briefs
1042	U+1FA73	🩳	🩳	🩳	🩳	🩳	🩳	🩳	🩳	—	—	—	—	shorts
1043	U+1F459	👙	👙	👙	👙	👙	👙	👙	👙	👙	👙	—	👙	bikini
1044	U+1F45A	👚	👚	👚	👚	👚	👚	👚	👚	👚	—	—	👚	woman’s clothes
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1045	U+1F45B	👛	👛	👛	👛	👛	👛	👛	👛	👛	—	👛	👛	purse
1046	U+1F45C	👜	👜	👜	👜	👜	👜	👜	👜	👜	👜	👜	👜	handbag
1047	U+1F45D	👝	👝	👝	👝	👝	👝	👝	👝	👝	—	👝	—	clutch bag
1048	U+1F6CD	🛍	🛍	🛍	🛍	🛍	🛍	🛍	🛍	—	—	—	—	shopping bags
1049	U+1F392	🎒	🎒	🎒	🎒	🎒	🎒	🎒	🎒	🎒	🎒	—	🎒	backpack
1050	U+1F45E	👞	👞	👞	👞	👞	👞	👞	👞	👞	—	—	👞	man’s shoe
1051	U+1F45F	👟	👟	👟	👟	👟	👟	👟	👟	👟	👟	👟	👟	running shoe
1052	U+1F97E	🥾	🥾	🥾	🥾	🥾	🥾	🥾	🥾	—	—	—	—	hiking boot
1053	U+1F97F	🥿	🥿	🥿	🥿	🥿	🥿	🥿	🥿	—	—	—	—	flat shoe
1054	U+1F460	👠	👠	👠	👠	👠	👠	👠	👠	👠	👠	👠	👠	high-heeled shoe
1055	U+1F461	👡	👡	👡	👡	👡	👡	👡	👡	👡	👡	—	—	woman’s sandal
1056	U+1FA70	🩰	🩰	🩰	🩰	🩰	🩰	🩰	🩰	—	—	—	—	ballet shoes
1057	U+1F462	👢	👢	👢	👢	👢	👢	👢	👢	👢	👢	—	👢	woman’s boot
1058	U+1F451	👑	👑	👑	👑	👑	👑	👑	👑	👑	👑	👑	👑	crown
1059	U+1F452	👒	👒	👒	👒	👒	👒	👒	👒	👒	👒	—	👒	woman’s hat
1060	U+1F3A9	🎩	🎩	🎩	🎩	🎩	🎩	🎩	🎩	🎩	🎩	🎩	🎩	top hat
1061	U+1F393	🎓	🎓	🎓	🎓	🎓	🎓	🎓	🎓	🎓	🎓	—	🎓	graduation cap
1062	U+1F9E2	🧢	🧢	🧢	🧢	🧢	🧢	🧢	🧢	—	—	—	—	billed cap
1063	U+26D1	⛑	⛑	⛑	⛑	⛑	⛑	⛑	⛑	—	—	—	—	rescue worker’s helmet
1064	U+1F4FF	📿	📿	📿	📿	📿	📿	📿	📿	—	—	—	—	prayer beads
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1065	U+1F484	💄	💄	💄	💄	💄	💄	💄	💄	💄	💄	💄	💄	lipstick
1066	U+1F48D	💍	💍	💍	💍	💍	💍	💍	💍	💍	💍	💍	💍	ring
1067	U+1F48E	💎	💎	💎	💎	💎	💎	💎	💎	💎	💎	—	—	gem stone
sound
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1068	U+1F507	🔇	🔇	🔇	🔇	🔇	🔇	🔇	🔇	—	—	—	—	muted speaker
1069	U+1F508	🔈	🔈	🔈	🔈	🔈	🔈	🔈	🔈	—	—	—	—	speaker low volume
1070	U+1F509	🔉	🔉	🔉	🔉	🔉	🔉	🔉	🔉	—	—	—	—	speaker medium volume
1071	U+1F50A	🔊	🔊	🔊	🔊	🔊	🔊	🔊	🔊	🔊	🔊	—	🔊	speaker high volume
1072	U+1F4E2	📢	📢	📢	📢	📢	📢	📢	📢	📢	📢	—	—	loudspeaker
1073	U+1F4E3	📣	📣	📣	📣	📣	📣	📣	📣	📣	📣	—	—	megaphone
1074	U+1F4EF	📯	📯	📯	📯	📯	📯	📯	📯	—	—	—	—	postal horn
1075	U+1F514	🔔	🔔	🔔	🔔	🔔	🔔	🔔	🔔	🔔	🔔	🔔	🔔	bell
1076	U+1F515	🔕	🔕	🔕	🔕	🔕	🔕	🔕	🔕	—	—	—	—	bell with slash
music
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1077	U+1F3BC	🎼	🎼	🎼	🎼	🎼	🎼	🎼	🎼	🎼	—	—	🎼	musical score
1078	U+1F3B5	🎵	🎵	🎵	🎵	🎵	🎵	🎵	🎵	🎵	🎵	🎵	🎵	musical note
1079	U+1F3B6	🎶	🎶	🎶	🎶	🎶	🎶	🎶	🎶	🎶	🎶	🎶	🎶	musical notes
1080	U+1F399	🎙	🎙	🎙	🎙	🎙	🎙	🎙	🎙	—	—	—	—	studio microphone
1081	U+1F39A	🎚	🎚	🎚	🎚	🎚	🎚	🎚	🎚	—	—	—	—	level slider
1082	U+1F39B	🎛	🎛	🎛	🎛	🎛	🎛	🎛	🎛	—	—	—	—	control knobs
1083	U+1F3A4	🎤	🎤	🎤	🎤	🎤	🎤	🎤	🎤	🎤	🎤	🎤	🎤	microphone
1084	U+1F3A7	🎧	🎧	🎧	🎧	🎧	🎧	🎧	🎧	🎧	🎧	🎧	🎧	headphone
1085	U+1F4FB	📻	📻	📻	📻	📻	📻	📻	📻	📻	📻	—	📻	radio
musical-instrument
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1086	U+1F3B7	🎷	🎷	🎷	🎷	🎷	🎷	🎷	🎷	🎷	🎷	—	—	saxophone
1087	U+1F3B8	🎸	🎸	🎸	🎸	🎸	🎸	🎸	🎸	🎸	🎸	—	🎸	guitar
1088	U+1F3B9	🎹	🎹	🎹	🎹	🎹	🎹	🎹	🎹	🎹	—	—	🎹	musical keyboard
1089	U+1F3BA	🎺	🎺	🎺	🎺	🎺	🎺	🎺	🎺	🎺	🎺	—	🎺	trumpet
1090	U+1F3BB	🎻	🎻	🎻	🎻	🎻	🎻	🎻	🎻	🎻	—	—	🎻	violin
1091	U+1FA95	🪕	🪕	🪕	🪕	🪕	🪕	🪕	🪕	—	—	—	—	banjo
1092	U+1F941	🥁	🥁	🥁	🥁	🥁	🥁	🥁	🥁	—	—	—	—	drum
phone
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1093	U+1F4F1	📱	📱	📱	📱	📱	📱	📱	📱	📱	📱	📱	📱	mobile phone
1094	U+1F4F2	📲	📲	📲	📲	📲	📲	📲	📲	📲	📲	📲	📲	mobile phone with arrow
1095	U+260E	☎	☎	☎	☎	☎	☎	☎	☎	☎	☎	☎	☎	telephone
1096	U+1F4DE	📞	📞	📞	📞	📞	📞	📞	📞	📞	—	—	📞	telephone receiver
1097	U+1F4DF	📟	📟	📟	📟	📟	📟	📟	📟	📟	—	📟	📟	pager
1098	U+1F4E0	📠	📠	📠	📠	📠	📠	📠	📠	📠	📠	📠	📠	fax machine
computer
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1099	U+1F50B	🔋	🔋	🔋	🔋	🔋	🔋	🔋	🔋	🔋	—	—	🔋	battery
1100	U+1F50C	🔌	🔌	🔌	🔌	🔌	🔌	🔌	🔌	🔌	—	—	🔌	electric plug
1101	U+1F4BB	💻	💻	💻	💻	💻	💻	💻	💻	💻	💻	💻	💻	laptop
1102	U+1F5A5	🖥	🖥	🖥	🖥	🖥	🖥	🖥	🖥	—	—	—	—	desktop computer
1103	U+1F5A8	🖨	🖨	🖨	🖨	🖨	🖨	🖨	🖨	—	—	—	—	printer
1104	U+2328	⌨	⌨	⌨	⌨	⌨	⌨	⌨	⌨	—	—	—	—	keyboard
1105	U+1F5B1	🖱	🖱	🖱	🖱	🖱	🖱	🖱	🖱	—	—	—	—	computer mouse
1106	U+1F5B2	🖲	🖲	🖲	🖲	🖲	🖲	🖲	🖲	—	—	—	—	trackball
1107	U+1F4BD	💽	💽	💽	💽	💽	💽	💽	💽	💽	💽	—	💽	computer disk
1108	U+1F4BE	💾	💾	💾	💾	💾	💾	💾	💾	💾	—	—	💾	floppy disk
1109	U+1F4BF	💿	💿	💿	💿	💿	💿	💿	💿	💿	💿	💿	💿	optical disk
1110	U+1F4C0	📀	📀	📀	📀	📀	📀	📀	📀	📀	📀	—	—	dvd
1111	U+1F9EE	🧮	🧮	🧮	🧮	🧮	🧮	🧮	🧮	—	—	—	—	abacus
light & video
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1112	U+1F3A5	🎥	🎥	🎥	🎥	🎥	🎥	🎥	🎥	🎥	🎥	🎥	🎥	movie camera
1113	U+1F39E	🎞	🎞	🎞	🎞	🎞	🎞	🎞	🎞	—	—	—	—	film frames
1114	U+1F4FD	📽	📽	📽	📽	📽	📽	📽	📽	—	—	—	—	film projector
1115	U+1F3AC	🎬	🎬	🎬	🎬	🎬	🎬	🎬	🎬	🎬	🎬	🎬	🎬	clapper board
1116	U+1F4FA	📺	📺	📺	📺	📺	📺	📺	📺	📺	📺	📺	📺	television
1117	U+1F4F7	📷	📷	📷	📷	📷	📷	📷	📷	📷	📷	📷	📷	camera
1118	U+1F4F8	📸	📸	📸	📸	📸	📸	📸	📸	—	—	—	—	camera with flash
1119	U+1F4F9	📹	📹	📹	📹	📹	📹	📹	📹	📹	—	—	📹	video camera
1120	U+1F4FC	📼	📼	📼	📼	📼	📼	📼	📼	📼	📼	—	📼	videocassette
1121	U+1F50D	🔍	🔍	🔍	🔍	🔍	🔍	🔍	🔍	🔍	🔍	🔍	🔍	magnifying glass tilted left
1122	U+1F50E	🔎	🔎	🔎	🔎	🔎	🔎	🔎	🔎	🔎	—	—	🔎	magnifying glass tilted right
1123	U+1F56F	🕯	🕯	🕯	🕯	🕯	🕯	🕯	🕯	—	—	—	—	candle
1124	U+1F4A1	💡	💡	💡	💡	💡	💡	💡	💡	💡	💡	💡	💡	light bulb
1125	U+1F526	🔦	🔦	🔦	🔦	🔦	🔦	🔦	🔦	🔦	—	—	🔦	flashlight
1126	U+1F3EE	🏮	🏮	🏮	🏮	🏮	🏮	🏮	🏮	🏮	—	—	🏮	red paper lantern
1127	U+1FA94	🪔	🪔	🪔	🪔	🪔	🪔	🪔	🪔	—	—	—	—	diya lamp
book-paper
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1128	U+1F4D4	📔	📔	📔	📔	📔	📔	📔	📔	📔	—	—	📔	notebook with decorative cover
1129	U+1F4D5	📕	📕	📕	📕	📕	📕	📕	📕	📕	—	—	📕	closed book
1130	U+1F4D6	📖	📖	📖	📖	📖	📖	📖	📖	📖	📖	📖	📖	open book
1131	U+1F4D7	📗	📗	📗	📗	📗	📗	📗	📗	📗	—	—	📗	green book
1132	U+1F4D8	📘	📘	📘	📘	📘	📘	📘	📘	📘	—	—	📘	blue book
1133	U+1F4D9	📙	📙	📙	📙	📙	📙	📙	📙	📙	—	—	📙	orange book
1134	U+1F4DA	📚	📚	📚	📚	📚	📚	📚	📚	📚	—	—	📚	books
1135	U+1F4D3	📓	📓	📓	📓	📓	📓	📓	📓	📓	—	—	📓	notebook
1136	U+1F4D2	📒	📒	📒	📒	📒	📒	📒	📒	📒	—	—	📒	ledger
1137	U+1F4C3	📃	📃	📃	📃	📃	📃	📃	📃	📃	—	—	📃	page with curl
1138	U+1F4DC	📜	📜	📜	📜	📜	📜	📜	📜	📜	—	—	📜	scroll
1139	U+1F4C4	📄	📄	📄	📄	📄	📄	📄	📄	📄	—	—	📄	page facing up
1140	U+1F4F0	📰	📰	📰	📰	📰	📰	📰	📰	📰	—	—	📰	newspaper
1141	U+1F5DE	🗞	🗞	🗞	🗞	🗞	🗞	🗞	🗞	—	—	—	—	rolled-up newspaper
1142	U+1F4D1	📑	📑	📑	📑	📑	📑	📑	📑	📑	—	—	📑	bookmark tabs
1143	U+1F516	🔖	🔖	🔖	🔖	🔖	🔖	🔖	🔖	🔖	—	—	🔖	bookmark
1144	U+1F3F7	🏷	🏷	🏷	🏷	🏷	🏷	🏷	🏷	—	—	—	—	label
money
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1145	U+1F4B0	💰	💰	💰	💰	💰	💰	💰	💰	💰	💰	💰	💰	money bag
1146	U+1F4B4	💴	💴	💴	💴	💴	💴	💴	💴	💴	—	💴	💴	yen banknote
1147	U+1F4B5	💵	💵	💵	💵	💵	💵	💵	💵	💵	—	—	💵	dollar banknote
1148	U+1F4B6	💶	💶	💶	💶	💶	💶	💶	💶	—	—	—	—	euro banknote
1149	U+1F4B7	💷	💷	💷	💷	💷	💷	💷	💷	—	—	—	—	pound banknote
1150	U+1F4B8	💸	💸	💸	💸	💸	💸	💸	💸	💸	—	—	💸	money with wings
1151	U+1F4B3	💳	💳	💳	💳	💳	💳	💳	💳	💳	—	—	💳	credit card
1152	U+1F9FE	🧾	🧾	🧾	🧾	🧾	🧾	🧾	🧾	—	—	—	—	receipt
1153	U+1F4B9	💹	💹	💹	💹	💹	💹	💹	💹	💹	💹	—	💹	chart increasing with yen
mail
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1154	U+2709	✉	✉	✉	✉	✉	✉	✉	✉	✉	—	✉	✉	envelope
1155	U+1F4E7	📧	📧	📧	📧	📧	📧	📧	📧	📧	—	—	📧	e-mail
1156	U+1F4E8	📨	📨	📨	📨	📨	📨	📨	📨	📨	—	—	📨	incoming envelope
1157	U+1F4E9	📩	📩	📩	📩	📩	📩	📩	📩	📩	📩	📩	📩	envelope with arrow
1158	U+1F4E4	📤	📤	📤	📤	📤	📤	📤	📤	📤	—	—	📤	outbox tray
1159	U+1F4E5	📥	📥	📥	📥	📥	📥	📥	📥	📥	—	—	📥	inbox tray
1160	U+1F4E6	📦	📦	📦	📦	📦	📦	📦	📦	📦	—	—	📦	package
1161	U+1F4EB	📫	📫	📫	📫	📫	📫	📫	📫	📫	📫	—	📫	closed mailbox with raised flag
1162	U+1F4EA	📪	📪	📪	📪	📪	📪	📪	📪	📪	—	—	📪	closed mailbox with lowered flag
1163	U+1F4EC	📬	📬	📬	📬	📬	📬	📬	📬	—	—	—	—	open mailbox with raised flag
1164	U+1F4ED	📭	📭	📭	📭	📭	📭	📭	📭	—	—	—	—	open mailbox with lowered flag
1165	U+1F4EE	📮	📮	📮	📮	📮	📮	📮	📮	📮	📮	—	—	postbox
1166	U+1F5F3	🗳	🗳	🗳	🗳	🗳	🗳	🗳	🗳	—	—	—	—	ballot box with ballot
writing
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1167	U+270F	✏	✏	✏	✏	✏	✏	✏	✏	✏	—	✏	✏	pencil
1168	U+2712	✒	✒	✒	✒	✒	✒	✒	✒	✒	—	✒	✒	black nib
1169	U+1F58B	🖋	🖋	🖋	🖋	🖋	🖋	🖋	🖋	—	—	—	—	fountain pen
1170	U+1F58A	🖊	🖊	🖊	🖊	🖊	🖊	🖊	🖊	—	—	—	—	pen
1171	U+1F58C	🖌	🖌	🖌	🖌	🖌	🖌	🖌	🖌	—	—	—	—	paintbrush
1172	U+1F58D	🖍	🖍	🖍	🖍	🖍	🖍	🖍	🖍	—	—	—	—	crayon
1173	U+1F4DD	📝	📝	📝	📝	📝	📝	📝	📝	📝	📝	📝	📝	memo
office
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1174	U+1F4BC	💼	💼	💼	💼	💼	💼	💼	💼	💼	💼	—	💼	briefcase
1175	U+1F4C1	📁	📁	📁	📁	📁	📁	📁	📁	📁	—	—	📁	file folder
1176	U+1F4C2	📂	📂	📂	📂	📂	📂	📂	📂	📂	—	—	📂	open file folder
1177	U+1F5C2	🗂	🗂	🗂	🗂	🗂	🗂	🗂	🗂	—	—	—	—	card index dividers
1178	U+1F4C5	📅	📅	📅	📅	📅	📅	📅	📅	📅	—	—	📅	calendar
1179	U+1F4C6	📆	📆	📆	📆	📆	📆	📆	📆	📆	—	—	📆	tear-off calendar
1180	U+1F5D2	🗒	🗒	🗒	🗒	🗒	🗒	🗒	🗒	—	—	—	—	spiral notepad
1181	U+1F5D3	🗓	🗓	🗓	🗓	🗓	🗓	🗓	🗓	—	—	—	—	spiral calendar
1182	U+1F4C7	📇	📇	📇	📇	📇	📇	📇	📇	📇	—	—	📇	card index
1183	U+1F4C8	📈	📈	📈	📈	📈	📈	📈	📈	📈	—	—	📈	chart increasing
1184	U+1F4C9	📉	📉	📉	📉	📉	📉	📉	📉	📉	—	—	📉	chart decreasing
1185	U+1F4CA	📊	📊	📊	📊	📊	📊	📊	📊	📊	—	—	📊	bar chart
1186	U+1F4CB	📋	📋	📋	📋	📋	📋	📋	📋	📋	—	—	📋	clipboard
1187	U+1F4CC	📌	📌	📌	📌	📌	📌	📌	📌	📌	—	—	📌	pushpin
1188	U+1F4CD	📍	📍	📍	📍	📍	📍	📍	📍	📍	—	—	📍	round pushpin
1189	U+1F4CE	📎	📎	📎	📎	📎	📎	📎	📎	📎	—	📎	📎	paperclip
1190	U+1F587	🖇	🖇	🖇	🖇	🖇	🖇	🖇	🖇	—	—	—	—	linked paperclips
1191	U+1F4CF	📏	📏	📏	📏	📏	📏	📏	📏	📏	—	—	📏	straight ruler
1192	U+1F4D0	📐	📐	📐	📐	📐	📐	📐	📐	📐	—	—	📐	triangular ruler
1193	U+2702	✂	✂	✂	✂	✂	✂	✂	✂	✂	✂	✂	✂	scissors
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1194	U+1F5C3	🗃	🗃	🗃	🗃	🗃	🗃	🗃	🗃	—	—	—	—	card file box
1195	U+1F5C4	🗄	🗄	🗄	🗄	🗄	🗄	🗄	🗄	—	—	—	—	file cabinet
1196	U+1F5D1	🗑	🗑	🗑	🗑	🗑	🗑	🗑	🗑	—	—	—	—	wastebasket
lock
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1197	U+1F512	🔒	🔒	🔒	🔒	🔒	🔒	🔒	🔒	🔒	🔒	—	🔒	locked
1198	U+1F513	🔓	🔓	🔓	🔓	🔓	🔓	🔓	🔓	🔓	🔓	—	—	unlocked
1199	U+1F50F	🔏	🔏	🔏	🔏	🔏	🔏	🔏	🔏	🔏	—	—	🔏	locked with pen
1200	U+1F510	🔐	🔐	🔐	🔐	🔐	🔐	🔐	🔐	🔐	—	—	🔐	locked with key
1201	U+1F511	🔑	🔑	🔑	🔑	🔑	🔑	🔑	🔑	🔑	🔑	🔑	🔑	key
1202	U+1F5DD	🗝	🗝	🗝	🗝	🗝	🗝	🗝	🗝	—	—	—	—	old key
tool
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1203	U+1F528	🔨	🔨	🔨	🔨	🔨	🔨	🔨	🔨	🔨	🔨	—	🔨	hammer
1204	U+1FA93	🪓	🪓	🪓	🪓	🪓	🪓	🪓	🪓	—	—	—	—	axe
1205	U+26CF	⛏	⛏	⛏	⛏	⛏	⛏	⛏	⛏	—	—	—	—	pick
1206	U+2692	⚒	⚒	⚒	⚒	⚒	⚒	⚒	⚒	—	—	—	—	hammer and pick
1207	U+1F6E0	🛠	🛠	🛠	🛠	🛠	🛠	🛠	🛠	—	—	—	—	hammer and wrench
1208	U+1F5E1	🗡	🗡	🗡	🗡	🗡	🗡	🗡	🗡	—	—	—	—	dagger
1209	U+2694	⚔	⚔	⚔	⚔	⚔	⚔	⚔	⚔	—	—	—	—	crossed swords
1210	U+1F52B	🔫	🔫	🔫	🔫	🔫	🔫	🔫	🔫	🔫	🔫	—	🔫	pistol
1211	U+1F3F9	🏹	🏹	🏹	🏹	🏹	🏹	🏹	🏹	—	—	—	—	bow and arrow
1212	U+1F6E1	🛡	🛡	🛡	🛡	🛡	🛡	🛡	🛡	—	—	—	—	shield
1213	U+1F527	🔧	🔧	🔧	🔧	🔧	🔧	🔧	🔧	🔧	—	🔧	🔧	wrench
1214	U+1F529	🔩	🔩	🔩	🔩	🔩	🔩	🔩	🔩	🔩	—	—	🔩	nut and bolt
1215	U+2699	⚙	⚙	⚙	⚙	⚙	⚙	⚙	⚙	—	—	—	—	gear
1216	U+1F5DC	🗜	🗜	🗜	🗜	🗜	🗜	🗜	🗜	—	—	—	—	clamp
1217	U+2696	⚖	⚖	⚖	⚖	⚖	⚖	⚖	⚖	—	—	—	—	balance scale
1218	U+1F9AF	🦯	🦯	🦯	🦯	🦯	🦯	🦯	🦯	—	—	—	—	white cane
1219	U+1F517	🔗	🔗	🔗	🔗	🔗	🔗	🔗	🔗	🔗	—	—	🔗	link
1220	U+26D3	⛓	⛓	⛓	⛓	⛓	⛓	⛓	⛓	—	—	—	—	chains
1221	U+1F9F0	🧰	🧰	🧰	🧰	🧰	🧰	🧰	🧰	—	—	—	—	toolbox
1222	U+1F9F2	🧲	🧲	🧲	🧲	🧲	🧲	🧲	🧲	—	—	—	—	magnet
science
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1223	U+2697	⚗	⚗	⚗	⚗	⚗	⚗	⚗	⚗	—	—	—	—	alembic
1224	U+1F9EA	🧪	🧪	🧪	🧪	🧪	🧪	🧪	🧪	—	—	—	—	test tube
1225	U+1F9EB	🧫	🧫	🧫	🧫	🧫	🧫	🧫	🧫	—	—	—	—	petri dish
1226	U+1F9EC	🧬	🧬	🧬	🧬	🧬	🧬	🧬	🧬	—	—	—	—	dna
1227	U+1F52C	🔬	🔬	🔬	🔬	🔬	🔬	🔬	🔬	—	—	—	—	microscope
1228	U+1F52D	🔭	🔭	🔭	🔭	🔭	🔭	🔭	🔭	—	—	—	—	telescope
1229	U+1F4E1	📡	📡	📡	📡	📡	📡	📡	📡	📡	📡	—	📡	satellite antenna
medical
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1230	U+1F489	💉	💉	💉	💉	💉	💉	💉	💉	💉	💉	—	💉	syringe
1231	U+1FA78	🩸	🩸	🩸	🩸	🩸	🩸	🩸	🩸	—	—	—	—	drop of blood
1232	U+1F48A	💊	💊	💊	💊	💊	💊	💊	💊	💊	💊	—	💊	pill
1233	U+1FA79	🩹	🩹	🩹	🩹	🩹	🩹	🩹	🩹	—	—	—	—	adhesive bandage
1234	U+1FA7A	🩺	🩺	🩺	🩺	🩺	🩺	🩺	🩺	—	—	—	—	stethoscope
household
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1235	U+1F6AA	🚪	🚪	🚪	🚪	🚪	🚪	🚪	🚪	🚪	—	🚪	—	door
1236	U+1F6CF	🛏	🛏	🛏	🛏	🛏	🛏	🛏	🛏	—	—	—	—	bed
1237	U+1F6CB	🛋	🛋	🛋	🛋	🛋	🛋	🛋	🛋	—	—	—	—	couch and lamp
1238	U+1FA91	🪑	🪑	🪑	🪑	🪑	🪑	🪑	🪑	—	—	—	—	chair
1239	U+1F6BD	🚽	🚽	🚽	🚽	🚽	🚽	🚽	🚽	🚽	🚽	—	—	toilet
1240	U+1F6BF	🚿	🚿	🚿	🚿	🚿	🚿	🚿	🚿	—	—	—	—	shower
1241	U+1F6C1	🛁	🛁	🛁	🛁	🛁	🛁	🛁	🛁	—	—	—	—	bathtub
1242	U+1FA92	🪒	🪒	🪒	🪒	🪒	🪒	🪒	🪒	—	—	—	—	razor
1243	U+1F9F4	🧴	🧴	🧴	🧴	🧴	🧴	🧴	🧴	—	—	—	—	lotion bottle
1244	U+1F9F7	🧷	🧷	🧷	🧷	🧷	🧷	🧷	🧷	—	—	—	—	safety pin
1245	U+1F9F9	🧹	🧹	🧹	🧹	🧹	🧹	🧹	🧹	—	—	—	—	broom
1246	U+1F9FA	🧺	🧺	🧺	🧺	🧺	🧺	🧺	🧺	—	—	—	—	basket
1247	U+1F9FB	🧻	🧻	🧻	🧻	🧻	🧻	🧻	🧻	—	—	—	—	roll of paper
1248	U+1F9FC	🧼	🧼	🧼	🧼	🧼	🧼	🧼	🧼	—	—	—	—	soap
1249	U+1F9FD	🧽	🧽	🧽	🧽	🧽	🧽	🧽	🧽	—	—	—	—	sponge
1250	U+1F9EF	🧯	🧯	🧯	🧯	🧯	🧯	🧯	🧯	—	—	—	—	fire extinguisher
1251	U+1F6D2	🛒	🛒	🛒	🛒	🛒	🛒	🛒	🛒	—	—	—	—	shopping cart
other-object
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1252	U+1F6AC	🚬	🚬	🚬	🚬	🚬	🚬	🚬	🚬	🚬	🚬	🚬	🚬	cigarette
1253	U+26B0	⚰	⚰	⚰	⚰	⚰	⚰	⚰	⚰	—	—	—	—	coffin
1254	U+26B1	⚱	⚱	⚱	⚱	⚱	⚱	⚱	⚱	—	—	—	—	funeral urn
1255	U+1F5FF	🗿	🗿	🗿	🗿	🗿	🗿	🗿	🗿	🗿	—	—	🗿	moai
Symbols
transport-sign
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1256	U+1F3E7	🏧	🏧	🏧	🏧	🏧	🏧	🏧	🏧	🏧	🏧	🏧	🏧	ATM sign
1257	U+1F6AE	🚮	🚮	🚮	🚮	🚮	🚮	🚮	🚮	—	—	—	—	litter in bin sign
1258	U+1F6B0	🚰	🚰	🚰	🚰	🚰	🚰	🚰	🚰	—	—	—	—	potable water
1259	U+267F	♿	♿	♿	♿	♿	♿	♿	♿	♿	♿	♿	♿	wheelchair symbol
1260	U+1F6B9	🚹	🚹	🚹	🚹	🚹	🚹	🚹	🚹	🚹	🚹	—	—	men’s room
1261	U+1F6BA	🚺	🚺	🚺	🚺	🚺	🚺	🚺	🚺	🚺	🚺	—	—	women’s room
1262	U+1F6BB	🚻	🚻	🚻	🚻	🚻	🚻	🚻	🚻	🚻	🚻	🚻	🚻	restroom
1263	U+1F6BC	🚼	🚼	🚼	🚼	🚼	🚼	🚼	🚼	🚼	🚼	—	—	baby symbol
1264	U+1F6BE	🚾	🚾	🚾	🚾	🚾	🚾	🚾	🚾	🚾	🚾	—	—	water closet
1265	U+1F6C2	🛂	🛂	🛂	🛂	🛂	🛂	🛂	🛂	—	—	—	—	passport control
1266	U+1F6C3	🛃	🛃	🛃	🛃	🛃	🛃	🛃	🛃	—	—	—	—	customs
1267	U+1F6C4	🛄	🛄	🛄	🛄	🛄	🛄	🛄	🛄	—	—	—	—	baggage claim
1268	U+1F6C5	🛅	🛅	🛅	🛅	🛅	🛅	🛅	🛅	—	—	—	—	left luggage
warning
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1269	U+26A0	⚠	⚠	⚠	⚠	⚠	⚠	⚠	⚠	⚠	⚠	⚠	⚠	warning
1270	U+1F6B8	🚸	🚸	🚸	🚸	🚸	🚸	🚸	🚸	—	—	—	—	children crossing
1271	U+26D4	⛔	⛔	⛔	⛔	⛔	⛔	⛔	⛔	⛔	—	—	⛔	no entry
1272	U+1F6AB	🚫	🚫	🚫	🚫	🚫	🚫	🚫	🚫	🚫	—	—	🚫	prohibited
1273	U+1F6B3	🚳	🚳	🚳	🚳	🚳	🚳	🚳	🚳	—	—	—	—	no bicycles
1274	U+1F6AD	🚭	🚭	🚭	🚭	🚭	🚭	🚭	🚭	🚭	🚭	🚭	🚭	no smoking
1275	U+1F6AF	🚯	🚯	🚯	🚯	🚯	🚯	🚯	🚯	—	—	—	—	no littering
1276	U+1F6B1	🚱	🚱	🚱	🚱	🚱	🚱	🚱	🚱	—	—	—	—	non-potable water
1277	U+1F6B7	🚷	🚷	🚷	🚷	🚷	🚷	🚷	🚷	—	—	—	—	no pedestrians
1278	U+1F4F5	📵	📵	📵	📵	📵	📵	📵	📵	—	—	—	—	no mobile phones
1279	U+1F51E	🔞	🔞	🔞	🔞	🔞	🔞	🔞	🔞	🔞	🔞	—	🔞	no one under eighteen
1280	U+2622	☢	☢	☢	☢	☢	☢	☢	☢	—	—	—	—	radioactive
1281	U+2623	☣	☣	☣	☣	☣	☣	☣	☣	—	—	—	—	biohazard
arrow
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1282	U+2B06	⬆	⬆	⬆	⬆	⬆	⬆	⬆	⬆	—	⬆	—	⬆	up arrow
1283	U+2197	↗	↗	↗	↗	↗	↗	↗	↗	↗	↗	↗	↗	up-right arrow
1284	U+27A1	➡	➡	➡	➡	➡	➡	➡	➡	—	➡	—	➡	right arrow
1285	U+2198	↘	↘	↘	↘	↘	↘	↘	↘	↘	↘	↘	↘	down-right arrow
1286	U+2B07	⬇	⬇	⬇	⬇	⬇	⬇	⬇	⬇	—	⬇	—	⬇	down arrow
1287	U+2199	↙	↙	↙	↙	↙	↙	↙	↙	↙	↙	↙	↙	down-left arrow
1288	U+2B05	⬅	⬅	⬅	⬅	⬅	⬅	⬅	⬅	—	⬅	—	⬅	left arrow
1289	U+2196	↖	↖	↖	↖	↖	↖	↖	↖	↖	↖	↖	↖	up-left arrow
1290	U+2195	↕	↕	↕	↕	↕	↕	↕	↕	↕	—	↕	↕	up-down arrow
1291	U+2194	↔	↔	↔	↔	↔	↔	↔	↔	↔	—	↔	↔	left-right arrow
1292	U+21A9	↩	↩	↩	↩	↩	↩	↩	↩	—	—	↩	↩	right arrow curving left
1293	U+21AA	↪	↪	↪	↪	↪	↪	↪	↪	↪	—	—	↪	left arrow curving right
1294	U+2934	⤴	⤴	⤴	⤴	⤴	⤴	⤴	⤴	⤴	—	⤴	⤴	right arrow curving up
1295	U+2935	⤵	⤵	⤵	⤵	⤵	⤵	⤵	⤵	⤵	—	⤵	⤵	right arrow curving down
1296	U+1F503	🔃	🔃	🔃	🔃	🔃	🔃	🔃	🔃	🔃	—	—	🔃	clockwise vertical arrows
1297	U+1F504	🔄	🔄	🔄	🔄	🔄	🔄	🔄	🔄	—	—	—	—	counterclockwise arrows button
1298	U+1F519	🔙	🔙	🔙	🔙	🔙	🔙	🔙	🔙	🔙	—	—	🔙	BACK arrow
1299	U+1F51A	🔚	🔚	🔚	🔚	🔚	🔚	🔚	🔚	🔚	—	🔚	—	END arrow
1300	U+1F51B	🔛	🔛	🔛	🔛	🔛	🔛	🔛	🔛	🔛	—	🔛	—	ON! arrow
1301	U+1F51C	🔜	🔜	🔜	🔜	🔜	🔜	🔜	🔜	🔜	—	🔜	—	SOON arrow
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1302	U+1F51D	🔝	🔝	🔝	🔝	🔝	🔝	🔝	🔝	🔝	🔝	—	—	TOP arrow
religion
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1303	U+1F6D0	🛐	🛐	🛐	🛐	🛐	🛐	🛐	🛐	—	—	—	—	place of worship
1304	U+269B	⚛	⚛	⚛	⚛	⚛	⚛	⚛	⚛	—	—	—	—	atom symbol
1305	U+1F549	🕉	🕉	🕉	🕉	🕉	🕉	🕉	🕉	—	—	—	—	om
1306	U+2721	✡	✡	✡	✡	✡	✡	✡	✡	—	—	—	—	star of David
1307	U+2638	☸	☸	☸	☸	☸	☸	☸	☸	—	—	—	—	wheel of dharma
1308	U+262F	☯	☯	☯	☯	☯	☯	☯	☯	—	—	—	—	yin yang
1309	U+271D	✝	✝	✝	✝	✝	✝	✝	✝	—	—	—	—	latin cross
1310	U+2626	☦	☦	☦	☦	☦	☦	☦	☦	—	—	—	—	orthodox cross
1311	U+262A	☪	☪	☪	☪	☪	☪	☪	☪	—	—	—	—	star and crescent
1312	U+262E	☮	☮	☮	☮	☮	☮	☮	☮	—	—	—	—	peace symbol
1313	U+1F54E	🕎	🕎	🕎	🕎	🕎	🕎	🕎	🕎	—	—	—	—	menorah
1314	U+1F52F	🔯	🔯	🔯	🔯	🔯	🔯	🔯	🔯	🔯	🔯	—	—	dotted six-pointed star
zodiac
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1315	U+2648	♈	♈	♈	♈	♈	♈	♈	♈	♈	♈	♈	♈	Aries
1316	U+2649	♉	♉	♉	♉	♉	♉	♉	♉	♉	♉	♉	♉	Taurus
1317	U+264A	♊	♊	♊	♊	♊	♊	♊	♊	♊	♊	♊	♊	Gemini
1318	U+264B	♋	♋	♋	♋	♋	♋	♋	♋	♋	♋	♋	♋	Cancer
1319	U+264C	♌	♌	♌	♌	♌	♌	♌	♌	♌	♌	♌	♌	Leo
1320	U+264D	♍	♍	♍	♍	♍	♍	♍	♍	♍	♍	♍	♍	Virgo
1321	U+264E	♎	♎	♎	♎	♎	♎	♎	♎	♎	♎	♎	♎	Libra
1322	U+264F	♏	♏	♏	♏	♏	♏	♏	♏	♏	♏	♏	♏	Scorpio
1323	U+2650	♐	♐	♐	♐	♐	♐	♐	♐	♐	♐	♐	♐	Sagittarius
1324	U+2651	♑	♑	♑	♑	♑	♑	♑	♑	♑	♑	♑	♑	Capricorn
1325	U+2652	♒	♒	♒	♒	♒	♒	♒	♒	♒	♒	♒	♒	Aquarius
1326	U+2653	♓	♓	♓	♓	♓	♓	♓	♓	♓	♓	♓	♓	Pisces
1327	U+26CE	⛎	⛎	⛎	⛎	⛎	⛎	⛎	⛎	⛎	⛎	—	⛎	Ophiuchus
av-symbol
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1328	U+1F500	🔀	🔀	🔀	🔀	🔀	🔀	🔀	🔀	—	—	—	—	shuffle tracks button
1329	U+1F501	🔁	🔁	🔁	🔁	🔁	🔁	🔁	🔁	—	—	—	—	repeat button
1330	U+1F502	🔂	🔂	🔂	🔂	🔂	🔂	🔂	🔂	—	—	—	—	repeat single button
1331	U+25B6	▶	▶	▶	▶	▶	▶	▶	▶	▶	▶	—	▶	play button
1332	U+23E9	⏩	⏩	⏩	⏩	⏩	⏩	⏩	⏩	⏩	⏩	—	⏩	fast-forward button
1333	U+23ED	⏭	⏭	⏭	⏭	⏭	⏭	⏭	⏭	—	—	—	—	next track button
1334	U+23EF	⏯	⏯	⏯	⏯	⏯	⏯	⏯	⏯	—	—	—	—	play or pause button
1335	U+25C0	◀	◀	◀	◀	◀	◀	◀	◀	◀	◀	—	◀	reverse button
1336	U+23EA	⏪	⏪	⏪	⏪	⏪	⏪	⏪	⏪	⏪	⏪	—	⏪	fast reverse button
1337	U+23EE	⏮	⏮	⏮	⏮	⏮	⏮	⏮	⏮	—	—	—	—	last track button
1338	U+1F53C	🔼	🔼	🔼	🔼	🔼	🔼	🔼	🔼	🔼	—	—	🔼	upwards button
1339	U+23EB	⏫	⏫	⏫	⏫	⏫	⏫	⏫	⏫	⏫	—	—	⏫	fast up button
1340	U+1F53D	🔽	🔽	🔽	🔽	🔽	🔽	🔽	🔽	🔽	—	—	🔽	downwards button
1341	U+23EC	⏬	⏬	⏬	⏬	⏬	⏬	⏬	⏬	⏬	—	—	⏬	fast down button
1342	U+23F8	⏸	⏸	⏸	⏸	⏸	⏸	⏸	⏸	—	—	—	—	pause button
1343	U+23F9	⏹	⏹	⏹	⏹	⏹	⏹	⏹	⏹	—	—	—	—	stop button
1344	U+23FA	⏺	⏺	⏺	⏺	⏺	⏺	⏺	⏺	—	—	—	—	record button
1345	U+23CF	⏏	⏏	⏏	⏏	⏏	⏏	⏏	⏏	—	—	—	—	eject button
1346	U+1F3A6	🎦	🎦	🎦	🎦	🎦	🎦	🎦	🎦	🎦	🎦	—	—	cinema
1347	U+1F505	🔅	🔅	🔅	🔅	🔅	🔅	🔅	🔅	—	—	—	—	dim button
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1348	U+1F506	🔆	🔆	🔆	🔆	🔆	🔆	🔆	🔆	—	—	—	—	bright button
1349	U+1F4F6	📶	📶	📶	📶	📶	📶	📶	📶	📶	📶	—	📶	antenna bars
1350	U+1F4F3	📳	📳	📳	📳	📳	📳	📳	📳	📳	📳	—	📳	vibration mode
1351	U+1F4F4	📴	📴	📴	📴	📴	📴	📴	📴	📴	📴	—	📴	mobile phone off
gender
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1352	U+2640	♀	—	♀	♀	♀	♀	♀	♀	—	—	—	—	female sign
1353	U+2642	♂	—	♂	♂	♂	♂	♂	♂	—	—	—	—	male sign
math
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1354	U+2716	✖	✖	✖	✖	✖	✖	✖	✖	—	—	—	✖	multiply
1355	U+2795	➕	➕	➕	➕	➕	➕	➕	➕	➕	—	—	➕	plus
1356	U+2796	➖	➖	➖	➖	➖	➖	➖	➖	➖	—	—	➖	minus
1357	U+2797	➗	➗	➗	➗	➗	➗	➗	➗	➗	—	—	➗	divide
1358	U+267E	♾	♾	♾	♾	♾	♾	♾	♾	—	—	—	—	infinity
punctuation
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1359	U+203C	‼	‼	‼	‼	‼	‼	‼	‼	‼	—	‼	‼	double exclamation mark
1360	U+2049	⁉	⁉	⁉	⁉	⁉	⁉	⁉	⁉	⁉	—	⁉	⁉	exclamation question mark
1361	U+2753	❓	❓	❓	❓	❓	❓	❓	❓	❓	❓	—	❓	question mark
1362	U+2754	❔	❔	❔	❔	❔	❔	❔	❔	❔	❔	—	—	white question mark
1363	U+2755	❕	❕	❕	❕	❕	❕	❕	❕	❕	❕	—	—	white exclamation mark
1364	U+2757	❗	❗	❗	❗	❗	❗	❗	❗	❗	❗	❗	❗	exclamation mark
1365	U+3030	〰	〰	〰	〰	〰	〰	〰	〰	〰	—	〰	—	wavy dash
currency
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1366	U+1F4B1	💱	💱	💱	💱	💱	💱	💱	💱	💱	💱	—	—	currency exchange
1367	U+1F4B2	💲	💲	💲	💲	💲	💲	💲	💲	💲	—	—	💲	heavy dollar sign
other-symbol
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1368	U+2695	⚕	—	⚕	⚕	⚕	⚕	⚕	⚕	—	—	—	—	medical symbol
1369	U+267B	♻	♻	♻	♻	♻	♻	♻	♻	♻	—	♻	♻	recycling symbol
1370	U+269C	⚜	⚜	⚜	⚜	⚜	⚜	⚜	⚜	—	—	—	—	fleur-de-lis
1371	U+1F531	🔱	🔱	🔱	🔱	🔱	🔱	🔱	🔱	🔱	🔱	—	—	trident emblem
1372	U+1F4DB	📛	📛	📛	📛	📛	📛	📛	📛	📛	—	—	📛	name badge
1373	U+1F530	🔰	🔰	🔰	🔰	🔰	🔰	🔰	🔰	🔰	🔰	—	🔰	Japanese symbol for beginner
1374	U+2B55	⭕	⭕	⭕	⭕	⭕	⭕	⭕	⭕	⭕	⭕	—	⭕	hollow red circle
1375	U+2705	✅	✅	✅	✅	✅	✅	✅	✅	✅	—	—	✅	check mark button
1376	U+2611	☑	☑	☑	☑	☑	☑	☑	☑	☑	—	—	☑	check box with check
1377	U+2714	✔	✔	✔	✔	✔	✔	✔	✔	—	—	—	✔	check mark
1378	U+274C	❌	❌	❌	❌	❌	❌	❌	❌	❌	❌	—	❌	cross mark
1379	U+274E	❎	❎	❎	❎	❎	❎	❎	❎	❎	—	—	❎	cross mark button
1380	U+27B0	➰	➰	➰	➰	➰	➰	➰	➰	➰	—	➰	➰	curly loop
1381	U+27BF	➿	➿	➿	➿	➿	➿	➿	➿	➿	—	—	—	double curly loop
1382	U+303D	〽	〽	〽	〽	〽	〽	〽	〽	〽	〽	—	—	part alternation mark
1383	U+2733	✳	✳	✳	✳	✳	✳	✳	✳	✳	✳	—	✳	eight-spoked asterisk
1384	U+2734	✴	✴	✴	✴	✴	✴	✴	✴	—	✴	—	✴	eight-pointed star
1385	U+2747	❇	❇	❇	❇	❇	❇	❇	❇	❇	—	—	❇	sparkle
1386	U+00A9	©	©	©	©	©	©	©	©	©	©	©	©	copyright
1387	U+00AE	®	®	®	®	®	®	®	®	®	®	®	®	registered
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1388	U+2122	™	™	™	™	™	™	™	™	™	™	™	™	trade mark
keycap
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1389	U+0023 U+FE0F U+20E3	#️⃣	#️⃣	#️⃣	#️⃣	#️⃣	#️⃣	#️⃣	#️⃣	#️⃣	#️⃣	#️⃣	#️⃣	keycap: #
1390	U+002A U+FE0F U+20E3	*️⃣	*️⃣	*️⃣	*️⃣	*️⃣	*️⃣	*️⃣	*️⃣	—	—	—	—	keycap: *
1391	U+0030 U+FE0F U+20E3	0️⃣	0️⃣	0️⃣	0️⃣	0️⃣	0️⃣	0️⃣	0️⃣	0️⃣	0️⃣	0️⃣	0️⃣	keycap: 0
1392	U+0031 U+FE0F U+20E3	1️⃣	1️⃣	1️⃣	1️⃣	1️⃣	1️⃣	1️⃣	1️⃣	1️⃣	1️⃣	1️⃣	1️⃣	keycap: 1
1393	U+0032 U+FE0F U+20E3	2️⃣	2️⃣	2️⃣	2️⃣	2️⃣	2️⃣	2️⃣	2️⃣	2️⃣	2️⃣	2️⃣	2️⃣	keycap: 2
1394	U+0033 U+FE0F U+20E3	3️⃣	3️⃣	3️⃣	3️⃣	3️⃣	3️⃣	3️⃣	3️⃣	3️⃣	3️⃣	3️⃣	3️⃣	keycap: 3
1395	U+0034 U+FE0F U+20E3	4️⃣	4️⃣	4️⃣	4️⃣	4️⃣	4️⃣	4️⃣	4️⃣	4️⃣	4️⃣	4️⃣	4️⃣	keycap: 4
1396	U+0035 U+FE0F U+20E3	5️⃣	5️⃣	5️⃣	5️⃣	5️⃣	5️⃣	5️⃣	5️⃣	5️⃣	5️⃣	5️⃣	5️⃣	keycap: 5
1397	U+0036 U+FE0F U+20E3	6️⃣	6️⃣	6️⃣	6️⃣	6️⃣	6️⃣	6️⃣	6️⃣	6️⃣	6️⃣	6️⃣	6️⃣	keycap: 6
1398	U+0037 U+FE0F U+20E3	7️⃣	7️⃣	7️⃣	7️⃣	7️⃣	7️⃣	7️⃣	7️⃣	7️⃣	7️⃣	7️⃣	7️⃣	keycap: 7
1399	U+0038 U+FE0F U+20E3	8️⃣	8️⃣	8️⃣	8️⃣	8️⃣	8️⃣	8️⃣	8️⃣	8️⃣	8️⃣	8️⃣	8️⃣	keycap: 8
1400	U+0039 U+FE0F U+20E3	9️⃣	9️⃣	9️⃣	9️⃣	9️⃣	9️⃣	9️⃣	9️⃣	9️⃣	9️⃣	9️⃣	9️⃣	keycap: 9
1401	U+1F51F	🔟	🔟	🔟	🔟	🔟	🔟	🔟	🔟	🔟	—	—	🔟	keycap: 10
alphanum
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1402	U+1F520	🔠	🔠	🔠	🔠	🔠	🔠	🔠	🔠	🔠	—	—	🔠	input latin uppercase
1403	U+1F521	🔡	🔡	🔡	🔡	🔡	🔡	🔡	🔡	🔡	—	—	🔡	input latin lowercase
1404	U+1F522	🔢	🔢	🔢	🔢	🔢	🔢	🔢	🔢	🔢	—	—	🔢	input numbers
1405	U+1F523	🔣	🔣	🔣	🔣	🔣	🔣	🔣	🔣	🔣	—	—	🔣	input symbols
1406	U+1F524	🔤	🔤	🔤	🔤	🔤	🔤	🔤	🔤	🔤	—	—	🔤	input latin letters
1407	U+1F170	🅰	🅰	🅰	🅰	🅰	🅰	🅰	🅰	🅰	🅰	—	🅰	A button (blood type)
1408	U+1F18E	🆎	🆎	🆎	🆎	🆎	🆎	🆎	🆎	🆎	🆎	—	🆎	AB button (blood type)
1409	U+1F171	🅱	🅱	🅱	🅱	🅱	🅱	🅱	🅱	🅱	🅱	—	🅱	B button (blood type)
1410	U+1F191	🆑	🆑	🆑	🆑	🆑	🆑	🆑	🆑	🆑	—	🆑	🆑	CL button
1411	U+1F192	🆒	🆒	🆒	🆒	🆒	🆒	🆒	🆒	🆒	🆒	—	🆒	COOL button
1412	U+1F193	🆓	🆓	🆓	🆓	🆓	🆓	🆓	🆓	🆓	—	🆓	🆓	FREE button
1413	U+2139	ℹ	ℹ	ℹ	ℹ	ℹ	ℹ	ℹ	ℹ	ℹ	—	—	ℹ	information
1414	U+1F194	🆔	🆔	🆔	🆔	🆔	🆔	🆔	🆔	🆔	🆔	🆔	🆔	ID button
1415	U+24C2	Ⓜ	Ⓜ	Ⓜ	Ⓜ	Ⓜ	Ⓜ	Ⓜ	Ⓜ	Ⓜ	—	Ⓜ	—	circled M
1416	U+1F195	🆕	🆕	🆕	🆕	🆕	🆕	🆕	🆕	🆕	🆕	🆕	🆕	NEW button
1417	U+1F196	🆖	🆖	🆖	🆖	🆖	🆖	🆖	🆖	🆖	—	🆖	—	NG button
1418	U+1F17E	🅾	🅾	🅾	🅾	🅾	🅾	🅾	🅾	🅾	🅾	—	🅾	O button (blood type)
1419	U+1F197	🆗	🆗	🆗	🆗	🆗	🆗	🆗	🆗	🆗	🆗	🆗	🆗	OK button
1420	U+1F17F	🅿	🅿	🅿	🅿	🅿	🅿	🅿	🅿	🅿	🅿	🅿	🅿	P button
1421	U+1F198	🆘	🆘	🆘	🆘	🆘	🆘	🆘	🆘	🆘	—	—	🆘	SOS button
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1422	U+1F199	🆙	🆙	🆙	🆙	🆙	🆙	🆙	🆙	🆙	🆙	—	🆙	UP! button
1423	U+1F19A	🆚	🆚	🆚	🆚	🆚	🆚	🆚	🆚	🆚	🆚	—	🆚	VS button
1424	U+1F201	🈁	🈁	🈁	🈁	🈁	🈁	🈁	🈁	🈁	🈁	—	—	Japanese “here” button
1425	U+1F202	🈂	🈂	🈂	🈂	🈂	🈂	🈂	🈂	🈂	🈂	—	🈂	Japanese “service charge” button
1426	U+1F237	🈷	🈷	🈷	🈷	🈷	🈷	🈷	🈷	🈷	🈷	—	—	Japanese “monthly amount” button
1427	U+1F236	🈶	🈶	🈶	🈶	🈶	🈶	🈶	🈶	🈶	🈶	—	—	Japanese “not free of charge” button
1428	U+1F22F	🈯	🈯	🈯	🈯	🈯	🈯	🈯	🈯	🈯	🈯	—	🈯	Japanese “reserved” button
1429	U+1F250	🉐	🉐	🉐	🉐	🉐	🉐	🉐	🉐	🉐	🉐	—	🉐	Japanese “bargain” button
1430	U+1F239	🈹	🈹	🈹	🈹	🈹	🈹	🈹	🈹	🈹	🈹	—	🈹	Japanese “discount” button
1431	U+1F21A	🈚	🈚	🈚	🈚	🈚	🈚	🈚	🈚	🈚	🈚	—	—	Japanese “free of charge” button
1432	U+1F232	🈲	🈲	🈲	🈲	🈲	🈲	🈲	🈲	🈲	—	🈲	—	Japanese “prohibited” button
1433	U+1F251	🉑	🉑	🉑	🉑	🉑	🉑	🉑	🉑	🉑	—	—	🉑	Japanese “acceptable” button
1434	U+1F238	🈸	🈸	🈸	🈸	🈸	🈸	🈸	🈸	🈸	🈸	—	—	Japanese “application” button
1435	U+1F234	🈴	🈴	🈴	🈴	🈴	🈴	🈴	🈴	🈴	—	🈴	—	Japanese “passing grade” button
1436	U+1F233	🈳	🈳	🈳	🈳	🈳	🈳	🈳	🈳	🈳	🈳	🈳	🈳	Japanese “vacancy” button
1437	U+3297	㊗	㊗	㊗	㊗	㊗	㊗	㊗	㊗	㊗	㊗	—	㊗	Japanese “congratulations” button
1438	U+3299	㊙	㊙	㊙	㊙	㊙	㊙	㊙	㊙	㊙	㊙	㊙	㊙	Japanese “secret” button
1439	U+1F23A	🈺	🈺	🈺	🈺	🈺	🈺	🈺	🈺	🈺	🈺	—	🈺	Japanese “open for business” button
1440	U+1F235	🈵	🈵	🈵	🈵	🈵	🈵	🈵	🈵	🈵	🈵	🈵	🈵	Japanese “no vacancy” button
geometric
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1441	U+1F534	🔴	🔴	🔴	🔴	🔴	🔴	🔴	🔴	🔴	🔴	—	🔴	red circle
1442	U+1F7E0	🟠	🟠	🟠	🟠	🟠	🟠	🟠	🟠	—	—	—	—	orange circle
1443	U+1F7E1	🟡	🟡	🟡	🟡	🟡	🟡	🟡	🟡	—	—	—	—	yellow circle
1444	U+1F7E2	🟢	🟢	🟢	🟢	🟢	🟢	🟢	🟢	—	—	—	—	green circle
1445	U+1F535	🔵	🔵	🔵	🔵	🔵	🔵	🔵	🔵	🔵	—	—	🔵	blue circle
1446	U+1F7E3	🟣	🟣	🟣	🟣	🟣	🟣	🟣	🟣	—	—	—	—	purple circle
1447	U+1F7E4	🟤	🟤	🟤	🟤	🟤	🟤	🟤	🟤	—	—	—	—	brown circle
1448	U+26AB	⚫	⚫	⚫	⚫	⚫	⚫	⚫	⚫	⚫	—	—	⚫	black circle
1449	U+26AA	⚪	⚪	⚪	⚪	⚪	⚪	⚪	⚪	⚪	—	—	⚪	white circle
1450	U+1F7E5	🟥	🟥	🟥	🟥	🟥	🟥	🟥	🟥	—	—	—	—	red square
1451	U+1F7E7	🟧	🟧	🟧	🟧	🟧	🟧	🟧	🟧	—	—	—	—	orange square
1452	U+1F7E8	🟨	🟨	🟨	🟨	🟨	🟨	🟨	🟨	—	—	—	—	yellow square
1453	U+1F7E9	🟩	🟩	🟩	🟩	🟩	🟩	🟩	🟩	—	—	—	—	green square
1454	U+1F7E6	🟦	🟦	🟦	🟦	🟦	🟦	🟦	🟦	—	—	—	—	blue square
1455	U+1F7EA	🟪	🟪	🟪	🟪	🟪	🟪	🟪	🟪	—	—	—	—	purple square
1456	U+1F7EB	🟫	🟫	🟫	🟫	🟫	🟫	🟫	🟫	—	—	—	—	brown square
1457	U+2B1B	⬛	⬛	⬛	⬛	⬛	⬛	⬛	⬛	—	—	—	⬛	black large square
1458	U+2B1C	⬜	⬜	⬜	⬜	⬜	⬜	⬜	⬜	—	—	—	⬜	white large square
1459	U+25FC	◼	◼	◼	◼	◼	◼	◼	◼	◼	—	—	◼	black medium square
1460	U+25FB	◻	◻	◻	◻	◻	◻	◻	◻	◻	—	—	◻	white medium square
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1461	U+25FE	◾	◾	◾	◾	◾	◾	◾	◾	◾	—	—	◾	black medium-small square
1462	U+25FD	◽	◽	◽	◽	◽	◽	◽	◽	◽	—	—	◽	white medium-small square
1463	U+25AA	▪	▪	▪	▪	▪	▪	▪	▪	▪	—	—	▪	black small square
1464	U+25AB	▫	▫	▫	▫	▫	▫	▫	▫	▫	—	—	▫	white small square
1465	U+1F536	🔶	🔶	🔶	🔶	🔶	🔶	🔶	🔶	🔶	—	—	🔶	large orange diamond
1466	U+1F537	🔷	🔷	🔷	🔷	🔷	🔷	🔷	🔷	🔷	—	—	🔷	large blue diamond
1467	U+1F538	🔸	🔸	🔸	🔸	🔸	🔸	🔸	🔸	🔸	—	—	🔸	small orange diamond
1468	U+1F539	🔹	🔹	🔹	🔹	🔹	🔹	🔹	🔹	🔹	—	—	🔹	small blue diamond
1469	U+1F53A	🔺	🔺	🔺	🔺	🔺	🔺	🔺	🔺	🔺	—	—	🔺	red triangle pointed up
1470	U+1F53B	🔻	🔻	🔻	🔻	🔻	🔻	🔻	🔻	🔻	—	—	🔻	red triangle pointed down
1471	U+1F4A0	💠	💠	💠	💠	💠	💠	💠	💠	💠	—	💠	—	diamond with a dot
1472	U+1F518	🔘	🔘	🔘	🔘	🔘	🔘	🔘	🔘	🔘	—	—	🔘	radio button
1473	U+1F533	🔳	🔳	🔳	🔳	🔳	🔳	🔳	🔳	🔳	🔳	—	—	white square button
1474	U+1F532	🔲	🔲	🔲	🔲	🔲	🔲	🔲	🔲	—	🔲	—	—	black square button
Flags
flag
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1475	U+1F3C1	🏁	🏁	🏁	🏁	🏁	🏁	🏁	🏁	🏁	🏁	🏁	🏁	chequered flag
1476	U+1F6A9	🚩	🚩	🚩	🚩	🚩	🚩	🚩	🚩	🚩	—	🚩	🚩	triangular flag
1477	U+1F38C	🎌	🎌	🎌	🎌	🎌	🎌	🎌	—	🎌	🎌	—	🎌	crossed flags
1478	U+1F3F4	🏴	🏴	🏴	🏴	🏴	🏴	🏴	🏴	—	—	—	—	black flag
1479	U+1F3F3	🏳	🏳	🏳	🏳	🏳	🏳	🏳	🏳	—	—	—	—	white flag
1480	U+1F3F3 U+FE0F U+200D U+1F308	🏳️‍🌈	🏳️‍🌈	🏳️‍🌈	🏳️‍🌈	🏳️‍🌈	🏳️‍🌈	🏳️‍🌈	🏳️‍🌈	—	—	—	—	rainbow flag
1481	U+1F3F4 U+200D U+2620 U+FE0F	🏴‍☠️	🏴‍☠️	🏴‍☠️	🏴‍☠️	🏴‍☠️	🏴‍☠️	🏴‍☠️	🏴‍☠️	—	—	—	—	pirate flag
country-flag
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1482	U+1F1E6 U+1F1E8	🇦🇨	🇦🇨	🇦🇨	🇦🇨	—	🇦🇨	🇦🇨	🇦🇨	—	—	—	—	flag: Ascension Island
1483	U+1F1E6 U+1F1E9	🇦🇩	🇦🇩	🇦🇩	🇦🇩	—	🇦🇩	🇦🇩	🇦🇩	—	—	—	—	flag: Andorra
1484	U+1F1E6 U+1F1EA	🇦🇪	🇦🇪	🇦🇪	🇦🇪	—	🇦🇪	🇦🇪	🇦🇪	—	—	—	—	flag: United Arab Emirates
1485	U+1F1E6 U+1F1EB	🇦🇫	🇦🇫	🇦🇫	🇦🇫	—	🇦🇫	🇦🇫	🇦🇫	—	—	—	—	flag: Afghanistan
1486	U+1F1E6 U+1F1EC	🇦🇬	🇦🇬	🇦🇬	🇦🇬	—	🇦🇬	🇦🇬	🇦🇬	—	—	—	—	flag: Antigua & Barbuda
1487	U+1F1E6 U+1F1EE	🇦🇮	🇦🇮	🇦🇮	🇦🇮	—	🇦🇮	🇦🇮	🇦🇮	—	—	—	—	flag: Anguilla
1488	U+1F1E6 U+1F1F1	🇦🇱	🇦🇱	🇦🇱	🇦🇱	—	🇦🇱	🇦🇱	🇦🇱	—	—	—	—	flag: Albania
1489	U+1F1E6 U+1F1F2	🇦🇲	🇦🇲	🇦🇲	🇦🇲	—	🇦🇲	🇦🇲	🇦🇲	—	—	—	—	flag: Armenia
1490	U+1F1E6 U+1F1F4	🇦🇴	🇦🇴	🇦🇴	🇦🇴	—	🇦🇴	🇦🇴	🇦🇴	—	—	—	—	flag: Angola
1491	U+1F1E6 U+1F1F6	🇦🇶	🇦🇶	🇦🇶	🇦🇶	—	🇦🇶	🇦🇶	🇦🇶	—	—	—	—	flag: Antarctica
1492	U+1F1E6 U+1F1F7	🇦🇷	🇦🇷	🇦🇷	🇦🇷	—	🇦🇷	🇦🇷	🇦🇷	—	—	—	—	flag: Argentina
1493	U+1F1E6 U+1F1F8	🇦🇸	🇦🇸	🇦🇸	🇦🇸	—	🇦🇸	🇦🇸	🇦🇸	—	—	—	—	flag: American Samoa
1494	U+1F1E6 U+1F1F9	🇦🇹	🇦🇹	🇦🇹	🇦🇹	—	🇦🇹	🇦🇹	🇦🇹	—	—	—	—	flag: Austria
1495	U+1F1E6 U+1F1FA	🇦🇺	🇦🇺	🇦🇺	🇦🇺	—	🇦🇺	🇦🇺	🇦🇺	—	—	—	—	flag: Australia
1496	U+1F1E6 U+1F1FC	🇦🇼	🇦🇼	🇦🇼	🇦🇼	—	🇦🇼	🇦🇼	🇦🇼	—	—	—	—	flag: Aruba
1497	U+1F1E6 U+1F1FD	🇦🇽	🇦🇽	🇦🇽	🇦🇽	—	🇦🇽	🇦🇽	🇦🇽	—	—	—	—	flag: Åland Islands
1498	U+1F1E6 U+1F1FF	🇦🇿	🇦🇿	🇦🇿	🇦🇿	—	🇦🇿	🇦🇿	🇦🇿	—	—	—	—	flag: Azerbaijan
1499	U+1F1E7 U+1F1E6	🇧🇦	🇧🇦	🇧🇦	🇧🇦	—	🇧🇦	🇧🇦	🇧🇦	—	—	—	—	flag: Bosnia & Herzegovina
1500	U+1F1E7 U+1F1E7	🇧🇧	🇧🇧	🇧🇧	🇧🇧	—	🇧🇧	🇧🇧	🇧🇧	—	—	—	—	flag: Barbados
1501	U+1F1E7 U+1F1E9	🇧🇩	🇧🇩	🇧🇩	🇧🇩	—	🇧🇩	🇧🇩	🇧🇩	—	—	—	—	flag: Bangladesh
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1502	U+1F1E7 U+1F1EA	🇧🇪	🇧🇪	🇧🇪	🇧🇪	—	🇧🇪	🇧🇪	🇧🇪	—	—	—	—	flag: Belgium
1503	U+1F1E7 U+1F1EB	🇧🇫	🇧🇫	🇧🇫	🇧🇫	—	🇧🇫	🇧🇫	🇧🇫	—	—	—	—	flag: Burkina Faso
1504	U+1F1E7 U+1F1EC	🇧🇬	🇧🇬	🇧🇬	🇧🇬	—	🇧🇬	🇧🇬	🇧🇬	—	—	—	—	flag: Bulgaria
1505	U+1F1E7 U+1F1ED	🇧🇭	🇧🇭	🇧🇭	🇧🇭	—	🇧🇭	🇧🇭	🇧🇭	—	—	—	—	flag: Bahrain
1506	U+1F1E7 U+1F1EE	🇧🇮	🇧🇮	🇧🇮	🇧🇮	—	🇧🇮	🇧🇮	🇧🇮	—	—	—	—	flag: Burundi
1507	U+1F1E7 U+1F1EF	🇧🇯	🇧🇯	🇧🇯	🇧🇯	—	🇧🇯	🇧🇯	🇧🇯	—	—	—	—	flag: Benin
1508	U+1F1E7 U+1F1F1	🇧🇱	🇧🇱	🇧🇱	🇧🇱	—	🇧🇱	🇧🇱	🇧🇱	—	—	—	—	flag: St. Barthélemy
1509	U+1F1E7 U+1F1F2	🇧🇲	🇧🇲	🇧🇲	🇧🇲	—	🇧🇲	🇧🇲	🇧🇲	—	—	—	—	flag: Bermuda
1510	U+1F1E7 U+1F1F3	🇧🇳	🇧🇳	🇧🇳	🇧🇳	—	🇧🇳	🇧🇳	🇧🇳	—	—	—	—	flag: Brunei
1511	U+1F1E7 U+1F1F4	🇧🇴	🇧🇴	🇧🇴	🇧🇴	—	🇧🇴	🇧🇴	🇧🇴	—	—	—	—	flag: Bolivia
1512	U+1F1E7 U+1F1F6	🇧🇶	🇧🇶	🇧🇶	🇧🇶	—	🇧🇶	🇧🇶	🇧🇶	—	—	—	—	flag: Caribbean Netherlands
1513	U+1F1E7 U+1F1F7	🇧🇷	🇧🇷	🇧🇷	🇧🇷	—	🇧🇷	🇧🇷	🇧🇷	—	—	—	—	flag: Brazil
1514	U+1F1E7 U+1F1F8	🇧🇸	🇧🇸	🇧🇸	🇧🇸	—	🇧🇸	🇧🇸	🇧🇸	—	—	—	—	flag: Bahamas
1515	U+1F1E7 U+1F1F9	🇧🇹	🇧🇹	🇧🇹	🇧🇹	—	🇧🇹	🇧🇹	🇧🇹	—	—	—	—	flag: Bhutan
1516	U+1F1E7 U+1F1FB	🇧🇻	🇧🇻	🇧🇻	🇧🇻	—	🇧🇻	🇧🇻	🇧🇻	—	—	—	—	flag: Bouvet Island
1517	U+1F1E7 U+1F1FC	🇧🇼	🇧🇼	🇧🇼	🇧🇼	—	🇧🇼	🇧🇼	🇧🇼	—	—	—	—	flag: Botswana
1518	U+1F1E7 U+1F1FE	🇧🇾	🇧🇾	🇧🇾	🇧🇾	—	🇧🇾	🇧🇾	🇧🇾	—	—	—	—	flag: Belarus
1519	U+1F1E7 U+1F1FF	🇧🇿	🇧🇿	🇧🇿	🇧🇿	—	🇧🇿	🇧🇿	🇧🇿	—	—	—	—	flag: Belize
1520	U+1F1E8 U+1F1E6	🇨🇦	🇨🇦	🇨🇦	🇨🇦	—	🇨🇦	🇨🇦	🇨🇦	—	—	—	—	flag: Canada
1521	U+1F1E8 U+1F1E8	🇨🇨	🇨🇨	🇨🇨	🇨🇨	—	🇨🇨	🇨🇨	🇨🇨	—	—	—	—	flag: Cocos (Keeling) Islands
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1522	U+1F1E8 U+1F1E9	🇨🇩	🇨🇩	🇨🇩	🇨🇩	—	🇨🇩	🇨🇩	🇨🇩	—	—	—	—	flag: Congo - Kinshasa
1523	U+1F1E8 U+1F1EB	🇨🇫	🇨🇫	🇨🇫	🇨🇫	—	🇨🇫	🇨🇫	🇨🇫	—	—	—	—	flag: Central African Republic
1524	U+1F1E8 U+1F1EC	🇨🇬	🇨🇬	🇨🇬	🇨🇬	—	🇨🇬	🇨🇬	🇨🇬	—	—	—	—	flag: Congo - Brazzaville
1525	U+1F1E8 U+1F1ED	🇨🇭	🇨🇭	🇨🇭	🇨🇭	—	🇨🇭	🇨🇭	🇨🇭	—	—	—	—	flag: Switzerland
1526	U+1F1E8 U+1F1EE	🇨🇮	🇨🇮	🇨🇮	🇨🇮	—	🇨🇮	🇨🇮	🇨🇮	—	—	—	—	flag: Côte d’Ivoire
1527	U+1F1E8 U+1F1F0	🇨🇰	🇨🇰	🇨🇰	🇨🇰	—	🇨🇰	🇨🇰	🇨🇰	—	—	—	—	flag: Cook Islands
1528	U+1F1E8 U+1F1F1	🇨🇱	🇨🇱	🇨🇱	🇨🇱	—	🇨🇱	🇨🇱	🇨🇱	—	—	—	—	flag: Chile
1529	U+1F1E8 U+1F1F2	🇨🇲	🇨🇲	🇨🇲	🇨🇲	—	🇨🇲	🇨🇲	🇨🇲	—	—	—	—	flag: Cameroon
1530	U+1F1E8 U+1F1F3	🇨🇳	🇨🇳	🇨🇳	🇨🇳	—	🇨🇳	🇨🇳	🇨🇳	🇨🇳	🇨🇳	—	🇨🇳	flag: China
1531	U+1F1E8 U+1F1F4	🇨🇴	🇨🇴	🇨🇴	🇨🇴	—	🇨🇴	🇨🇴	🇨🇴	—	—	—	—	flag: Colombia
1532	U+1F1E8 U+1F1F5	🇨🇵	🇨🇵	🇨🇵	🇨🇵	—	🇨🇵	🇨🇵	🇨🇵	—	—	—	—	flag: Clipperton Island
1533	U+1F1E8 U+1F1F7	🇨🇷	🇨🇷	🇨🇷	🇨🇷	—	🇨🇷	🇨🇷	🇨🇷	—	—	—	—	flag: Costa Rica
1534	U+1F1E8 U+1F1FA	🇨🇺	🇨🇺	🇨🇺	🇨🇺	—	🇨🇺	🇨🇺	🇨🇺	—	—	—	—	flag: Cuba
1535	U+1F1E8 U+1F1FB	🇨🇻	🇨🇻	🇨🇻	🇨🇻	—	🇨🇻	🇨🇻	🇨🇻	—	—	—	—	flag: Cape Verde
1536	U+1F1E8 U+1F1FC	🇨🇼	🇨🇼	🇨🇼	🇨🇼	—	🇨🇼	🇨🇼	🇨🇼	—	—	—	—	flag: Curaçao
1537	U+1F1E8 U+1F1FD	🇨🇽	🇨🇽	🇨🇽	🇨🇽	—	🇨🇽	🇨🇽	🇨🇽	—	—	—	—	flag: Christmas Island
1538	U+1F1E8 U+1F1FE	🇨🇾	🇨🇾	🇨🇾	🇨🇾	—	🇨🇾	🇨🇾	🇨🇾	—	—	—	—	flag: Cyprus
1539	U+1F1E8 U+1F1FF	🇨🇿	🇨🇿	🇨🇿	🇨🇿	—	🇨🇿	🇨🇿	🇨🇿	—	—	—	—	flag: Czechia
1540	U+1F1E9 U+1F1EA	🇩🇪	🇩🇪	🇩🇪	🇩🇪	—	🇩🇪	🇩🇪	🇩🇪	🇩🇪	🇩🇪	—	🇩🇪	flag: Germany
1541	U+1F1E9 U+1F1EC	🇩🇬	🇩🇬	🇩🇬	🇩🇬	—	🇩🇬	🇩🇬	🇩🇬	—	—	—	—	flag: Diego Garcia
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1542	U+1F1E9 U+1F1EF	🇩🇯	🇩🇯	🇩🇯	🇩🇯	—	🇩🇯	🇩🇯	🇩🇯	—	—	—	—	flag: Djibouti
1543	U+1F1E9 U+1F1F0	🇩🇰	🇩🇰	🇩🇰	🇩🇰	—	🇩🇰	🇩🇰	🇩🇰	—	—	—	—	flag: Denmark
1544	U+1F1E9 U+1F1F2	🇩🇲	🇩🇲	🇩🇲	🇩🇲	—	🇩🇲	🇩🇲	🇩🇲	—	—	—	—	flag: Dominica
1545	U+1F1E9 U+1F1F4	🇩🇴	🇩🇴	🇩🇴	🇩🇴	—	🇩🇴	🇩🇴	🇩🇴	—	—	—	—	flag: Dominican Republic
1546	U+1F1E9 U+1F1FF	🇩🇿	🇩🇿	🇩🇿	🇩🇿	—	🇩🇿	🇩🇿	🇩🇿	—	—	—	—	flag: Algeria
1547	U+1F1EA U+1F1E6	🇪🇦	🇪🇦	🇪🇦	🇪🇦	—	🇪🇦	🇪🇦	🇪🇦	—	—	—	—	flag: Ceuta & Melilla
1548	U+1F1EA U+1F1E8	🇪🇨	🇪🇨	🇪🇨	🇪🇨	—	🇪🇨	🇪🇨	🇪🇨	—	—	—	—	flag: Ecuador
1549	U+1F1EA U+1F1EA	🇪🇪	🇪🇪	🇪🇪	🇪🇪	—	🇪🇪	🇪🇪	🇪🇪	—	—	—	—	flag: Estonia
1550	U+1F1EA U+1F1EC	🇪🇬	🇪🇬	🇪🇬	🇪🇬	—	🇪🇬	🇪🇬	🇪🇬	—	—	—	—	flag: Egypt
1551	U+1F1EA U+1F1ED	🇪🇭	🇪🇭	🇪🇭	🇪🇭	—	🇪🇭	🇪🇭	🇪🇭	—	—	—	—	flag: Western Sahara
1552	U+1F1EA U+1F1F7	🇪🇷	🇪🇷	🇪🇷	🇪🇷	—	🇪🇷	🇪🇷	🇪🇷	—	—	—	—	flag: Eritrea
1553	U+1F1EA U+1F1F8	🇪🇸	🇪🇸	🇪🇸	🇪🇸	—	🇪🇸	🇪🇸	🇪🇸	🇪🇸	🇪🇸	—	🇪🇸	flag: Spain
1554	U+1F1EA U+1F1F9	🇪🇹	🇪🇹	🇪🇹	🇪🇹	—	🇪🇹	🇪🇹	🇪🇹	—	—	—	—	flag: Ethiopia
1555	U+1F1EA U+1F1FA	🇪🇺	🇪🇺	🇪🇺	🇪🇺	—	🇪🇺	🇪🇺	🇪🇺	—	—	—	—	flag: European Union
1556	U+1F1EB U+1F1EE	🇫🇮	🇫🇮	🇫🇮	🇫🇮	—	🇫🇮	🇫🇮	🇫🇮	—	—	—	—	flag: Finland
1557	U+1F1EB U+1F1EF	🇫🇯	🇫🇯	🇫🇯	🇫🇯	—	🇫🇯	🇫🇯	🇫🇯	—	—	—	—	flag: Fiji
1558	U+1F1EB U+1F1F0	🇫🇰	🇫🇰	🇫🇰	🇫🇰	—	🇫🇰	🇫🇰	🇫🇰	—	—	—	—	flag: Falkland Islands
1559	U+1F1EB U+1F1F2	🇫🇲	🇫🇲	🇫🇲	🇫🇲	—	🇫🇲	🇫🇲	🇫🇲	—	—	—	—	flag: Micronesia
1560	U+1F1EB U+1F1F4	🇫🇴	🇫🇴	🇫🇴	🇫🇴	—	🇫🇴	🇫🇴	🇫🇴	—	—	—	—	flag: Faroe Islands
1561	U+1F1EB U+1F1F7	🇫🇷	🇫🇷	🇫🇷	🇫🇷	—	🇫🇷	🇫🇷	🇫🇷	🇫🇷	🇫🇷	—	🇫🇷	flag: France
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1562	U+1F1EC U+1F1E6	🇬🇦	🇬🇦	🇬🇦	🇬🇦	—	🇬🇦	🇬🇦	🇬🇦	—	—	—	—	flag: Gabon
1563	U+1F1EC U+1F1E7	🇬🇧	🇬🇧	🇬🇧	🇬🇧	—	🇬🇧	🇬🇧	🇬🇧	🇬🇧	🇬🇧	—	🇬🇧	flag: United Kingdom
1564	U+1F1EC U+1F1E9	🇬🇩	🇬🇩	🇬🇩	🇬🇩	—	🇬🇩	🇬🇩	🇬🇩	—	—	—	—	flag: Grenada
1565	U+1F1EC U+1F1EA	🇬🇪	🇬🇪	🇬🇪	🇬🇪	—	🇬🇪	🇬🇪	🇬🇪	—	—	—	—	flag: Georgia
1566	U+1F1EC U+1F1EB	🇬🇫	🇬🇫	🇬🇫	🇬🇫	—	🇬🇫	🇬🇫	🇬🇫	—	—	—	—	flag: French Guiana
1567	U+1F1EC U+1F1EC	🇬🇬	🇬🇬	🇬🇬	🇬🇬	—	🇬🇬	🇬🇬	🇬🇬	—	—	—	—	flag: Guernsey
1568	U+1F1EC U+1F1ED	🇬🇭	🇬🇭	🇬🇭	🇬🇭	—	🇬🇭	🇬🇭	🇬🇭	—	—	—	—	flag: Ghana
1569	U+1F1EC U+1F1EE	🇬🇮	🇬🇮	🇬🇮	🇬🇮	—	🇬🇮	🇬🇮	🇬🇮	—	—	—	—	flag: Gibraltar
1570	U+1F1EC U+1F1F1	🇬🇱	🇬🇱	🇬🇱	🇬🇱	—	🇬🇱	🇬🇱	🇬🇱	—	—	—	—	flag: Greenland
1571	U+1F1EC U+1F1F2	🇬🇲	🇬🇲	🇬🇲	🇬🇲	—	🇬🇲	🇬🇲	🇬🇲	—	—	—	—	flag: Gambia
1572	U+1F1EC U+1F1F3	🇬🇳	🇬🇳	🇬🇳	🇬🇳	—	🇬🇳	🇬🇳	🇬🇳	—	—	—	—	flag: Guinea
1573	U+1F1EC U+1F1F5	🇬🇵	🇬🇵	🇬🇵	🇬🇵	—	🇬🇵	🇬🇵	🇬🇵	—	—	—	—	flag: Guadeloupe
1574	U+1F1EC U+1F1F6	🇬🇶	🇬🇶	🇬🇶	🇬🇶	—	🇬🇶	🇬🇶	🇬🇶	—	—	—	—	flag: Equatorial Guinea
1575	U+1F1EC U+1F1F7	🇬🇷	🇬🇷	🇬🇷	🇬🇷	—	🇬🇷	🇬🇷	🇬🇷	—	—	—	—	flag: Greece
1576	U+1F1EC U+1F1F8	🇬🇸	🇬🇸	🇬🇸	🇬🇸	—	🇬🇸	🇬🇸	🇬🇸	—	—	—	—	flag: South Georgia & South Sandwich Islands
1577	U+1F1EC U+1F1F9	🇬🇹	🇬🇹	🇬🇹	🇬🇹	—	🇬🇹	🇬🇹	🇬🇹	—	—	—	—	flag: Guatemala
1578	U+1F1EC U+1F1FA	🇬🇺	🇬🇺	🇬🇺	🇬🇺	—	🇬🇺	🇬🇺	🇬🇺	—	—	—	—	flag: Guam
1579	U+1F1EC U+1F1FC	🇬🇼	🇬🇼	🇬🇼	🇬🇼	—	🇬🇼	🇬🇼	🇬🇼	—	—	—	—	flag: Guinea-Bissau
1580	U+1F1EC U+1F1FE	🇬🇾	🇬🇾	🇬🇾	🇬🇾	—	🇬🇾	🇬🇾	🇬🇾	—	—	—	—	flag: Guyana
1581	U+1F1ED U+1F1F0	🇭🇰	🇭🇰	🇭🇰	🇭🇰	—	🇭🇰	🇭🇰	🇭🇰	—	—	—	—	flag: Hong Kong SAR China
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1582	U+1F1ED U+1F1F2	🇭🇲	🇭🇲	🇭🇲	🇭🇲	—	🇭🇲	🇭🇲	🇭🇲	—	—	—	—	flag: Heard & McDonald Islands
1583	U+1F1ED U+1F1F3	🇭🇳	🇭🇳	🇭🇳	🇭🇳	—	🇭🇳	🇭🇳	🇭🇳	—	—	—	—	flag: Honduras
1584	U+1F1ED U+1F1F7	🇭🇷	🇭🇷	🇭🇷	🇭🇷	—	🇭🇷	🇭🇷	🇭🇷	—	—	—	—	flag: Croatia
1585	U+1F1ED U+1F1F9	🇭🇹	🇭🇹	🇭🇹	🇭🇹	—	🇭🇹	🇭🇹	🇭🇹	—	—	—	—	flag: Haiti
1586	U+1F1ED U+1F1FA	🇭🇺	🇭🇺	🇭🇺	🇭🇺	—	🇭🇺	🇭🇺	🇭🇺	—	—	—	—	flag: Hungary
1587	U+1F1EE U+1F1E8	🇮🇨	🇮🇨	🇮🇨	🇮🇨	—	🇮🇨	🇮🇨	🇮🇨	—	—	—	—	flag: Canary Islands
1588	U+1F1EE U+1F1E9	🇮🇩	🇮🇩	🇮🇩	🇮🇩	—	🇮🇩	🇮🇩	🇮🇩	—	—	—	—	flag: Indonesia
1589	U+1F1EE U+1F1EA	🇮🇪	🇮🇪	🇮🇪	🇮🇪	—	🇮🇪	🇮🇪	🇮🇪	—	—	—	—	flag: Ireland
1590	U+1F1EE U+1F1F1	🇮🇱	🇮🇱	🇮🇱	🇮🇱	—	🇮🇱	🇮🇱	🇮🇱	—	—	—	—	flag: Israel
1591	U+1F1EE U+1F1F2	🇮🇲	🇮🇲	🇮🇲	🇮🇲	—	🇮🇲	🇮🇲	🇮🇲	—	—	—	—	flag: Isle of Man
1592	U+1F1EE U+1F1F3	🇮🇳	🇮🇳	🇮🇳	🇮🇳	—	🇮🇳	🇮🇳	🇮🇳	—	—	—	—	flag: India
1593	U+1F1EE U+1F1F4	🇮🇴	🇮🇴	🇮🇴	🇮🇴	—	🇮🇴	🇮🇴	🇮🇴	—	—	—	—	flag: British Indian Ocean Territory
1594	U+1F1EE U+1F1F6	🇮🇶	🇮🇶	🇮🇶	🇮🇶	—	🇮🇶	🇮🇶	🇮🇶	—	—	—	—	flag: Iraq
1595	U+1F1EE U+1F1F7	🇮🇷	🇮🇷	🇮🇷	🇮🇷	—	🇮🇷	🇮🇷	🇮🇷	—	—	—	—	flag: Iran
1596	U+1F1EE U+1F1F8	🇮🇸	🇮🇸	🇮🇸	🇮🇸	—	🇮🇸	🇮🇸	🇮🇸	—	—	—	—	flag: Iceland
1597	U+1F1EE U+1F1F9	🇮🇹	🇮🇹	🇮🇹	🇮🇹	—	🇮🇹	🇮🇹	🇮🇹	🇮🇹	🇮🇹	—	🇮🇹	flag: Italy
1598	U+1F1EF U+1F1EA	🇯🇪	🇯🇪	🇯🇪	🇯🇪	—	🇯🇪	🇯🇪	🇯🇪	—	—	—	—	flag: Jersey
1599	U+1F1EF U+1F1F2	🇯🇲	🇯🇲	🇯🇲	🇯🇲	—	🇯🇲	🇯🇲	🇯🇲	—	—	—	—	flag: Jamaica
1600	U+1F1EF U+1F1F4	🇯🇴	🇯🇴	🇯🇴	🇯🇴	—	🇯🇴	🇯🇴	🇯🇴	—	—	—	—	flag: Jordan
1601	U+1F1EF U+1F1F5	🇯🇵	🇯🇵	🇯🇵	🇯🇵	—	🇯🇵	🇯🇵	🇯🇵	🇯🇵	🇯🇵	—	🇯🇵	flag: Japan
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1602	U+1F1F0 U+1F1EA	🇰🇪	🇰🇪	🇰🇪	🇰🇪	—	🇰🇪	🇰🇪	🇰🇪	—	—	—	—	flag: Kenya
1603	U+1F1F0 U+1F1EC	🇰🇬	🇰🇬	🇰🇬	🇰🇬	—	🇰🇬	🇰🇬	🇰🇬	—	—	—	—	flag: Kyrgyzstan
1604	U+1F1F0 U+1F1ED	🇰🇭	🇰🇭	🇰🇭	🇰🇭	—	🇰🇭	🇰🇭	🇰🇭	—	—	—	—	flag: Cambodia
1605	U+1F1F0 U+1F1EE	🇰🇮	🇰🇮	🇰🇮	🇰🇮	—	🇰🇮	🇰🇮	🇰🇮	—	—	—	—	flag: Kiribati
1606	U+1F1F0 U+1F1F2	🇰🇲	🇰🇲	🇰🇲	🇰🇲	—	🇰🇲	🇰🇲	🇰🇲	—	—	—	—	flag: Comoros
1607	U+1F1F0 U+1F1F3	🇰🇳	🇰🇳	🇰🇳	🇰🇳	—	🇰🇳	🇰🇳	🇰🇳	—	—	—	—	flag: St. Kitts & Nevis
1608	U+1F1F0 U+1F1F5	🇰🇵	🇰🇵	🇰🇵	🇰🇵	—	🇰🇵	🇰🇵	🇰🇵	—	—	—	—	flag: North Korea
1609	U+1F1F0 U+1F1F7	🇰🇷	🇰🇷	🇰🇷	🇰🇷	—	🇰🇷	🇰🇷	🇰🇷	🇰🇷	🇰🇷	—	🇰🇷	flag: South Korea
1610	U+1F1F0 U+1F1FC	🇰🇼	🇰🇼	🇰🇼	🇰🇼	—	🇰🇼	🇰🇼	🇰🇼	—	—	—	—	flag: Kuwait
1611	U+1F1F0 U+1F1FE	🇰🇾	🇰🇾	🇰🇾	🇰🇾	—	🇰🇾	🇰🇾	🇰🇾	—	—	—	—	flag: Cayman Islands
1612	U+1F1F0 U+1F1FF	🇰🇿	🇰🇿	🇰🇿	🇰🇿	—	🇰🇿	🇰🇿	🇰🇿	—	—	—	—	flag: Kazakhstan
1613	U+1F1F1 U+1F1E6	🇱🇦	🇱🇦	🇱🇦	🇱🇦	—	🇱🇦	🇱🇦	🇱🇦	—	—	—	—	flag: Laos
1614	U+1F1F1 U+1F1E7	🇱🇧	🇱🇧	🇱🇧	🇱🇧	—	🇱🇧	🇱🇧	🇱🇧	—	—	—	—	flag: Lebanon
1615	U+1F1F1 U+1F1E8	🇱🇨	🇱🇨	🇱🇨	🇱🇨	—	🇱🇨	🇱🇨	🇱🇨	—	—	—	—	flag: St. Lucia
1616	U+1F1F1 U+1F1EE	🇱🇮	🇱🇮	🇱🇮	🇱🇮	—	🇱🇮	🇱🇮	🇱🇮	—	—	—	—	flag: Liechtenstein
1617	U+1F1F1 U+1F1F0	🇱🇰	🇱🇰	🇱🇰	🇱🇰	—	🇱🇰	🇱🇰	🇱🇰	—	—	—	—	flag: Sri Lanka
1618	U+1F1F1 U+1F1F7	🇱🇷	🇱🇷	🇱🇷	🇱🇷	—	🇱🇷	🇱🇷	🇱🇷	—	—	—	—	flag: Liberia
1619	U+1F1F1 U+1F1F8	🇱🇸	🇱🇸	🇱🇸	🇱🇸	—	🇱🇸	🇱🇸	🇱🇸	—	—	—	—	flag: Lesotho
1620	U+1F1F1 U+1F1F9	🇱🇹	🇱🇹	🇱🇹	🇱🇹	—	🇱🇹	🇱🇹	🇱🇹	—	—	—	—	flag: Lithuania
1621	U+1F1F1 U+1F1FA	🇱🇺	🇱🇺	🇱🇺	🇱🇺	—	🇱🇺	🇱🇺	🇱🇺	—	—	—	—	flag: Luxembourg
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1622	U+1F1F1 U+1F1FB	🇱🇻	🇱🇻	🇱🇻	🇱🇻	—	🇱🇻	🇱🇻	🇱🇻	—	—	—	—	flag: Latvia
1623	U+1F1F1 U+1F1FE	🇱🇾	🇱🇾	🇱🇾	🇱🇾	—	🇱🇾	🇱🇾	🇱🇾	—	—	—	—	flag: Libya
1624	U+1F1F2 U+1F1E6	🇲🇦	🇲🇦	🇲🇦	🇲🇦	—	🇲🇦	🇲🇦	🇲🇦	—	—	—	—	flag: Morocco
1625	U+1F1F2 U+1F1E8	🇲🇨	🇲🇨	🇲🇨	🇲🇨	—	🇲🇨	🇲🇨	🇲🇨	—	—	—	—	flag: Monaco
1626	U+1F1F2 U+1F1E9	🇲🇩	🇲🇩	🇲🇩	🇲🇩	—	🇲🇩	🇲🇩	🇲🇩	—	—	—	—	flag: Moldova
1627	U+1F1F2 U+1F1EA	🇲🇪	🇲🇪	🇲🇪	🇲🇪	—	🇲🇪	🇲🇪	🇲🇪	—	—	—	—	flag: Montenegro
1628	U+1F1F2 U+1F1EB	🇲🇫	🇲🇫	🇲🇫	🇲🇫	—	🇲🇫	🇲🇫	🇲🇫	—	—	—	—	flag: St. Martin
1629	U+1F1F2 U+1F1EC	🇲🇬	🇲🇬	🇲🇬	🇲🇬	—	🇲🇬	🇲🇬	🇲🇬	—	—	—	—	flag: Madagascar
1630	U+1F1F2 U+1F1ED	🇲🇭	🇲🇭	🇲🇭	🇲🇭	—	🇲🇭	🇲🇭	🇲🇭	—	—	—	—	flag: Marshall Islands
1631	U+1F1F2 U+1F1F0	🇲🇰	🇲🇰	🇲🇰	🇲🇰	—	🇲🇰	🇲🇰	🇲🇰	—	—	—	—	flag: North Macedonia
1632	U+1F1F2 U+1F1F1	🇲🇱	🇲🇱	🇲🇱	🇲🇱	—	🇲🇱	🇲🇱	🇲🇱	—	—	—	—	flag: Mali
1633	U+1F1F2 U+1F1F2	🇲🇲	🇲🇲	🇲🇲	🇲🇲	—	🇲🇲	🇲🇲	🇲🇲	—	—	—	—	flag: Myanmar (Burma)
1634	U+1F1F2 U+1F1F3	🇲🇳	🇲🇳	🇲🇳	🇲🇳	—	🇲🇳	🇲🇳	🇲🇳	—	—	—	—	flag: Mongolia
1635	U+1F1F2 U+1F1F4	🇲🇴	🇲🇴	🇲🇴	🇲🇴	—	🇲🇴	🇲🇴	🇲🇴	—	—	—	—	flag: Macao SAR China
1636	U+1F1F2 U+1F1F5	🇲🇵	🇲🇵	🇲🇵	🇲🇵	—	🇲🇵	🇲🇵	🇲🇵	—	—	—	—	flag: Northern Mariana Islands
1637	U+1F1F2 U+1F1F6	🇲🇶	🇲🇶	🇲🇶	🇲🇶	—	🇲🇶	🇲🇶	🇲🇶	—	—	—	—	flag: Martinique
1638	U+1F1F2 U+1F1F7	🇲🇷	🇲🇷	🇲🇷	🇲🇷	—	🇲🇷	🇲🇷	🇲🇷	—	—	—	—	flag: Mauritania
1639	U+1F1F2 U+1F1F8	🇲🇸	🇲🇸	🇲🇸	🇲🇸	—	🇲🇸	🇲🇸	🇲🇸	—	—	—	—	flag: Montserrat
1640	U+1F1F2 U+1F1F9	🇲🇹	🇲🇹	🇲🇹	🇲🇹	—	🇲🇹	🇲🇹	🇲🇹	—	—	—	—	flag: Malta
1641	U+1F1F2 U+1F1FA	🇲🇺	🇲🇺	🇲🇺	🇲🇺	—	🇲🇺	🇲🇺	🇲🇺	—	—	—	—	flag: Mauritius
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1642	U+1F1F2 U+1F1FB	🇲🇻	🇲🇻	🇲🇻	🇲🇻	—	🇲🇻	🇲🇻	🇲🇻	—	—	—	—	flag: Maldives
1643	U+1F1F2 U+1F1FC	🇲🇼	🇲🇼	🇲🇼	🇲🇼	—	🇲🇼	🇲🇼	🇲🇼	—	—	—	—	flag: Malawi
1644	U+1F1F2 U+1F1FD	🇲🇽	🇲🇽	🇲🇽	🇲🇽	—	🇲🇽	🇲🇽	🇲🇽	—	—	—	—	flag: Mexico
1645	U+1F1F2 U+1F1FE	🇲🇾	🇲🇾	🇲🇾	🇲🇾	—	🇲🇾	🇲🇾	🇲🇾	—	—	—	—	flag: Malaysia
1646	U+1F1F2 U+1F1FF	🇲🇿	🇲🇿	🇲🇿	🇲🇿	—	🇲🇿	🇲🇿	🇲🇿	—	—	—	—	flag: Mozambique
1647	U+1F1F3 U+1F1E6	🇳🇦	🇳🇦	🇳🇦	🇳🇦	—	🇳🇦	🇳🇦	🇳🇦	—	—	—	—	flag: Namibia
1648	U+1F1F3 U+1F1E8	🇳🇨	🇳🇨	🇳🇨	🇳🇨	—	🇳🇨	🇳🇨	🇳🇨	—	—	—	—	flag: New Caledonia
1649	U+1F1F3 U+1F1EA	🇳🇪	🇳🇪	🇳🇪	🇳🇪	—	🇳🇪	🇳🇪	🇳🇪	—	—	—	—	flag: Niger
1650	U+1F1F3 U+1F1EB	🇳🇫	🇳🇫	🇳🇫	🇳🇫	—	🇳🇫	🇳🇫	🇳🇫	—	—	—	—	flag: Norfolk Island
1651	U+1F1F3 U+1F1EC	🇳🇬	🇳🇬	🇳🇬	🇳🇬	—	🇳🇬	🇳🇬	🇳🇬	—	—	—	—	flag: Nigeria
1652	U+1F1F3 U+1F1EE	🇳🇮	🇳🇮	🇳🇮	🇳🇮	—	🇳🇮	🇳🇮	🇳🇮	—	—	—	—	flag: Nicaragua
1653	U+1F1F3 U+1F1F1	🇳🇱	🇳🇱	🇳🇱	🇳🇱	—	🇳🇱	🇳🇱	🇳🇱	—	—	—	—	flag: Netherlands
1654	U+1F1F3 U+1F1F4	🇳🇴	🇳🇴	🇳🇴	🇳🇴	—	🇳🇴	🇳🇴	🇳🇴	—	—	—	—	flag: Norway
1655	U+1F1F3 U+1F1F5	🇳🇵	🇳🇵	🇳🇵	🇳🇵	—	🇳🇵	🇳🇵	🇳🇵	—	—	—	—	flag: Nepal
1656	U+1F1F3 U+1F1F7	🇳🇷	🇳🇷	🇳🇷	🇳🇷	—	🇳🇷	🇳🇷	🇳🇷	—	—	—	—	flag: Nauru
1657	U+1F1F3 U+1F1FA	🇳🇺	🇳🇺	🇳🇺	🇳🇺	—	🇳🇺	🇳🇺	🇳🇺	—	—	—	—	flag: Niue
1658	U+1F1F3 U+1F1FF	🇳🇿	🇳🇿	🇳🇿	🇳🇿	—	🇳🇿	🇳🇿	🇳🇿	—	—	—	—	flag: New Zealand
1659	U+1F1F4 U+1F1F2	🇴🇲	🇴🇲	🇴🇲	🇴🇲	—	🇴🇲	🇴🇲	🇴🇲	—	—	—	—	flag: Oman
1660	U+1F1F5 U+1F1E6	🇵🇦	🇵🇦	🇵🇦	🇵🇦	—	🇵🇦	🇵🇦	🇵🇦	—	—	—	—	flag: Panama
1661	U+1F1F5 U+1F1EA	🇵🇪	🇵🇪	🇵🇪	🇵🇪	—	🇵🇪	🇵🇪	🇵🇪	—	—	—	—	flag: Peru
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1662	U+1F1F5 U+1F1EB	🇵🇫	🇵🇫	🇵🇫	🇵🇫	—	🇵🇫	🇵🇫	🇵🇫	—	—	—	—	flag: French Polynesia
1663	U+1F1F5 U+1F1EC	🇵🇬	🇵🇬	🇵🇬	🇵🇬	—	🇵🇬	🇵🇬	🇵🇬	—	—	—	—	flag: Papua New Guinea
1664	U+1F1F5 U+1F1ED	🇵🇭	🇵🇭	🇵🇭	🇵🇭	—	🇵🇭	🇵🇭	🇵🇭	—	—	—	—	flag: Philippines
1665	U+1F1F5 U+1F1F0	🇵🇰	🇵🇰	🇵🇰	🇵🇰	—	🇵🇰	🇵🇰	🇵🇰	—	—	—	—	flag: Pakistan
1666	U+1F1F5 U+1F1F1	🇵🇱	🇵🇱	🇵🇱	🇵🇱	—	🇵🇱	🇵🇱	🇵🇱	—	—	—	—	flag: Poland
1667	U+1F1F5 U+1F1F2	🇵🇲	🇵🇲	🇵🇲	🇵🇲	—	🇵🇲	🇵🇲	🇵🇲	—	—	—	—	flag: St. Pierre & Miquelon
1668	U+1F1F5 U+1F1F3	🇵🇳	🇵🇳	🇵🇳	🇵🇳	—	🇵🇳	🇵🇳	🇵🇳	—	—	—	—	flag: Pitcairn Islands
1669	U+1F1F5 U+1F1F7	🇵🇷	🇵🇷	🇵🇷	🇵🇷	—	🇵🇷	🇵🇷	🇵🇷	—	—	—	—	flag: Puerto Rico
1670	U+1F1F5 U+1F1F8	🇵🇸	🇵🇸	🇵🇸	🇵🇸	—	🇵🇸	🇵🇸	🇵🇸	—	—	—	—	flag: Palestinian Territories
1671	U+1F1F5 U+1F1F9	🇵🇹	🇵🇹	🇵🇹	🇵🇹	—	🇵🇹	🇵🇹	🇵🇹	—	—	—	—	flag: Portugal
1672	U+1F1F5 U+1F1FC	🇵🇼	🇵🇼	🇵🇼	🇵🇼	—	🇵🇼	🇵🇼	🇵🇼	—	—	—	—	flag: Palau
1673	U+1F1F5 U+1F1FE	🇵🇾	🇵🇾	🇵🇾	🇵🇾	—	🇵🇾	🇵🇾	🇵🇾	—	—	—	—	flag: Paraguay
1674	U+1F1F6 U+1F1E6	🇶🇦	🇶🇦	🇶🇦	🇶🇦	—	🇶🇦	🇶🇦	🇶🇦	—	—	—	—	flag: Qatar
1675	U+1F1F7 U+1F1EA	🇷🇪	🇷🇪	🇷🇪	🇷🇪	—	🇷🇪	🇷🇪	🇷🇪	—	—	—	—	flag: Réunion
1676	U+1F1F7 U+1F1F4	🇷🇴	🇷🇴	🇷🇴	🇷🇴	—	🇷🇴	🇷🇴	🇷🇴	—	—	—	—	flag: Romania
1677	U+1F1F7 U+1F1F8	🇷🇸	🇷🇸	🇷🇸	🇷🇸	—	🇷🇸	🇷🇸	🇷🇸	—	—	—	—	flag: Serbia
1678	U+1F1F7 U+1F1FA	🇷🇺	🇷🇺	🇷🇺	🇷🇺	—	🇷🇺	🇷🇺	🇷🇺	🇷🇺	🇷🇺	—	🇷🇺	flag: Russia
1679	U+1F1F7 U+1F1FC	🇷🇼	🇷🇼	🇷🇼	🇷🇼	—	🇷🇼	🇷🇼	🇷🇼	—	—	—	—	flag: Rwanda
1680	U+1F1F8 U+1F1E6	🇸🇦	🇸🇦	🇸🇦	🇸🇦	—	🇸🇦	🇸🇦	🇸🇦	—	—	—	—	flag: Saudi Arabia
1681	U+1F1F8 U+1F1E7	🇸🇧	🇸🇧	🇸🇧	🇸🇧	—	🇸🇧	🇸🇧	🇸🇧	—	—	—	—	flag: Solomon Islands
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1682	U+1F1F8 U+1F1E8	🇸🇨	🇸🇨	🇸🇨	🇸🇨	—	🇸🇨	🇸🇨	🇸🇨	—	—	—	—	flag: Seychelles
1683	U+1F1F8 U+1F1E9	🇸🇩	🇸🇩	🇸🇩	🇸🇩	—	🇸🇩	🇸🇩	🇸🇩	—	—	—	—	flag: Sudan
1684	U+1F1F8 U+1F1EA	🇸🇪	🇸🇪	🇸🇪	🇸🇪	—	🇸🇪	🇸🇪	🇸🇪	—	—	—	—	flag: Sweden
1685	U+1F1F8 U+1F1EC	🇸🇬	🇸🇬	🇸🇬	🇸🇬	—	🇸🇬	🇸🇬	🇸🇬	—	—	—	—	flag: Singapore
1686	U+1F1F8 U+1F1ED	🇸🇭	🇸🇭	🇸🇭	🇸🇭	—	🇸🇭	🇸🇭	🇸🇭	—	—	—	—	flag: St. Helena
1687	U+1F1F8 U+1F1EE	🇸🇮	🇸🇮	🇸🇮	🇸🇮	—	🇸🇮	🇸🇮	🇸🇮	—	—	—	—	flag: Slovenia
1688	U+1F1F8 U+1F1EF	🇸🇯	🇸🇯	🇸🇯	🇸🇯	—	🇸🇯	🇸🇯	🇸🇯	—	—	—	—	flag: Svalbard & Jan Mayen
1689	U+1F1F8 U+1F1F0	🇸🇰	🇸🇰	🇸🇰	🇸🇰	—	🇸🇰	🇸🇰	🇸🇰	—	—	—	—	flag: Slovakia
1690	U+1F1F8 U+1F1F1	🇸🇱	🇸🇱	🇸🇱	🇸🇱	—	🇸🇱	🇸🇱	🇸🇱	—	—	—	—	flag: Sierra Leone
1691	U+1F1F8 U+1F1F2	🇸🇲	🇸🇲	🇸🇲	🇸🇲	—	🇸🇲	🇸🇲	🇸🇲	—	—	—	—	flag: San Marino
1692	U+1F1F8 U+1F1F3	🇸🇳	🇸🇳	🇸🇳	🇸🇳	—	🇸🇳	🇸🇳	🇸🇳	—	—	—	—	flag: Senegal
1693	U+1F1F8 U+1F1F4	🇸🇴	🇸🇴	🇸🇴	🇸🇴	—	🇸🇴	🇸🇴	🇸🇴	—	—	—	—	flag: Somalia
1694	U+1F1F8 U+1F1F7	🇸🇷	🇸🇷	🇸🇷	🇸🇷	—	🇸🇷	🇸🇷	🇸🇷	—	—	—	—	flag: Suriname
1695	U+1F1F8 U+1F1F8	🇸🇸	🇸🇸	🇸🇸	🇸🇸	—	🇸🇸	🇸🇸	🇸🇸	—	—	—	—	flag: South Sudan
1696	U+1F1F8 U+1F1F9	🇸🇹	🇸🇹	🇸🇹	🇸🇹	—	🇸🇹	🇸🇹	🇸🇹	—	—	—	—	flag: São Tomé & Príncipe
1697	U+1F1F8 U+1F1FB	🇸🇻	🇸🇻	🇸🇻	🇸🇻	—	🇸🇻	🇸🇻	🇸🇻	—	—	—	—	flag: El Salvador
1698	U+1F1F8 U+1F1FD	🇸🇽	🇸🇽	🇸🇽	🇸🇽	—	🇸🇽	🇸🇽	🇸🇽	—	—	—	—	flag: Sint Maarten
1699	U+1F1F8 U+1F1FE	🇸🇾	🇸🇾	🇸🇾	🇸🇾	—	🇸🇾	🇸🇾	🇸🇾	—	—	—	—	flag: Syria
1700	U+1F1F8 U+1F1FF	🇸🇿	🇸🇿	🇸🇿	🇸🇿	—	🇸🇿	🇸🇿	🇸🇿	—	—	—	—	flag: Eswatini
1701	U+1F1F9 U+1F1E6	🇹🇦	🇹🇦	🇹🇦	🇹🇦	—	🇹🇦	🇹🇦	🇹🇦	—	—	—	—	flag: Tristan da Cunha
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1702	U+1F1F9 U+1F1E8	🇹🇨	🇹🇨	🇹🇨	🇹🇨	—	🇹🇨	🇹🇨	🇹🇨	—	—	—	—	flag: Turks & Caicos Islands
1703	U+1F1F9 U+1F1E9	🇹🇩	🇹🇩	🇹🇩	🇹🇩	—	🇹🇩	🇹🇩	🇹🇩	—	—	—	—	flag: Chad
1704	U+1F1F9 U+1F1EB	🇹🇫	🇹🇫	🇹🇫	🇹🇫	—	🇹🇫	🇹🇫	🇹🇫	—	—	—	—	flag: French Southern Territories
1705	U+1F1F9 U+1F1EC	🇹🇬	🇹🇬	🇹🇬	🇹🇬	—	🇹🇬	🇹🇬	🇹🇬	—	—	—	—	flag: Togo
1706	U+1F1F9 U+1F1ED	🇹🇭	🇹🇭	🇹🇭	🇹🇭	—	🇹🇭	🇹🇭	🇹🇭	—	—	—	—	flag: Thailand
1707	U+1F1F9 U+1F1EF	🇹🇯	🇹🇯	🇹🇯	🇹🇯	—	🇹🇯	🇹🇯	🇹🇯	—	—	—	—	flag: Tajikistan
1708	U+1F1F9 U+1F1F0	🇹🇰	🇹🇰	🇹🇰	🇹🇰	—	🇹🇰	🇹🇰	🇹🇰	—	—	—	—	flag: Tokelau
1709	U+1F1F9 U+1F1F1	🇹🇱	🇹🇱	🇹🇱	🇹🇱	—	🇹🇱	🇹🇱	🇹🇱	—	—	—	—	flag: Timor-Leste
1710	U+1F1F9 U+1F1F2	🇹🇲	🇹🇲	🇹🇲	🇹🇲	—	🇹🇲	🇹🇲	🇹🇲	—	—	—	—	flag: Turkmenistan
1711	U+1F1F9 U+1F1F3	🇹🇳	🇹🇳	🇹🇳	🇹🇳	—	🇹🇳	🇹🇳	🇹🇳	—	—	—	—	flag: Tunisia
1712	U+1F1F9 U+1F1F4	🇹🇴	🇹🇴	🇹🇴	🇹🇴	—	🇹🇴	🇹🇴	🇹🇴	—	—	—	—	flag: Tonga
1713	U+1F1F9 U+1F1F7	🇹🇷	🇹🇷	🇹🇷	🇹🇷	—	🇹🇷	🇹🇷	🇹🇷	—	—	—	—	flag: Turkey
1714	U+1F1F9 U+1F1F9	🇹🇹	🇹🇹	🇹🇹	🇹🇹	—	🇹🇹	🇹🇹	🇹🇹	—	—	—	—	flag: Trinidad & Tobago
1715	U+1F1F9 U+1F1FB	🇹🇻	🇹🇻	🇹🇻	🇹🇻	—	🇹🇻	🇹🇻	🇹🇻	—	—	—	—	flag: Tuvalu
1716	U+1F1F9 U+1F1FC	🇹🇼	🇹🇼	🇹🇼	🇹🇼	—	🇹🇼	🇹🇼	🇹🇼	—	—	—	—	flag: Taiwan
1717	U+1F1F9 U+1F1FF	🇹🇿	🇹🇿	🇹🇿	🇹🇿	—	🇹🇿	🇹🇿	🇹🇿	—	—	—	—	flag: Tanzania
1718	U+1F1FA U+1F1E6	🇺🇦	🇺🇦	🇺🇦	🇺🇦	—	🇺🇦	🇺🇦	🇺🇦	—	—	—	—	flag: Ukraine
1719	U+1F1FA U+1F1EC	🇺🇬	🇺🇬	🇺🇬	🇺🇬	—	🇺🇬	🇺🇬	🇺🇬	—	—	—	—	flag: Uganda
1720	U+1F1FA U+1F1F2	🇺🇲	🇺🇲	🇺🇲	🇺🇲	—	🇺🇲	🇺🇲	🇺🇲	—	—	—	—	flag: U.S. Outlying Islands
1721	U+1F1FA U+1F1F3	🇺🇳	🇺🇳	🇺🇳	🇺🇳	—	🇺🇳	🇺🇳	🇺🇳	—	—	—	—	flag: United Nations
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1722	U+1F1FA U+1F1F8	🇺🇸	🇺🇸	🇺🇸	🇺🇸	—	🇺🇸	🇺🇸	🇺🇸	🇺🇸	🇺🇸	—	🇺🇸	flag: United States
1723	U+1F1FA U+1F1FE	🇺🇾	🇺🇾	🇺🇾	🇺🇾	—	🇺🇾	🇺🇾	🇺🇾	—	—	—	—	flag: Uruguay
1724	U+1F1FA U+1F1FF	🇺🇿	🇺🇿	🇺🇿	🇺🇿	—	🇺🇿	🇺🇿	🇺🇿	—	—	—	—	flag: Uzbekistan
1725	U+1F1FB U+1F1E6	🇻🇦	🇻🇦	🇻🇦	🇻🇦	—	🇻🇦	🇻🇦	🇻🇦	—	—	—	—	flag: Vatican City
1726	U+1F1FB U+1F1E8	🇻🇨	🇻🇨	🇻🇨	🇻🇨	—	🇻🇨	🇻🇨	🇻🇨	—	—	—	—	flag: St. Vincent & Grenadines
1727	U+1F1FB U+1F1EA	🇻🇪	🇻🇪	🇻🇪	🇻🇪	—	🇻🇪	🇻🇪	🇻🇪	—	—	—	—	flag: Venezuela
1728	U+1F1FB U+1F1EC	🇻🇬	🇻🇬	🇻🇬	🇻🇬	—	🇻🇬	🇻🇬	🇻🇬	—	—	—	—	flag: British Virgin Islands
1729	U+1F1FB U+1F1EE	🇻🇮	🇻🇮	🇻🇮	🇻🇮	—	🇻🇮	🇻🇮	🇻🇮	—	—	—	—	flag: U.S. Virgin Islands
1730	U+1F1FB U+1F1F3	🇻🇳	🇻🇳	🇻🇳	🇻🇳	—	🇻🇳	🇻🇳	🇻🇳	—	—	—	—	flag: Vietnam
1731	U+1F1FB U+1F1FA	🇻🇺	🇻🇺	🇻🇺	🇻🇺	—	🇻🇺	🇻🇺	🇻🇺	—	—	—	—	flag: Vanuatu
1732	U+1F1FC U+1F1EB	🇼🇫	🇼🇫	🇼🇫	🇼🇫	—	🇼🇫	🇼🇫	🇼🇫	—	—	—	—	flag: Wallis & Futuna
1733	U+1F1FC U+1F1F8	🇼🇸	🇼🇸	🇼🇸	🇼🇸	—	🇼🇸	🇼🇸	🇼🇸	—	—	—	—	flag: Samoa
1734	U+1F1FD U+1F1F0	🇽🇰	🇽🇰	🇽🇰	🇽🇰	—	🇽🇰	🇽🇰	🇽🇰	—	—	—	—	flag: Kosovo
1735	U+1F1FE U+1F1EA	🇾🇪	🇾🇪	🇾🇪	🇾🇪	—	🇾🇪	🇾🇪	🇾🇪	—	—	—	—	flag: Yemen
1736	U+1F1FE U+1F1F9	🇾🇹	🇾🇹	🇾🇹	🇾🇹	—	🇾🇹	🇾🇹	🇾🇹	—	—	—	—	flag: Mayotte
1737	U+1F1FF U+1F1E6	🇿🇦	🇿🇦	🇿🇦	🇿🇦	—	🇿🇦	🇿🇦	🇿🇦	—	—	—	—	flag: South Africa
1738	U+1F1FF U+1F1F2	🇿🇲	🇿🇲	🇿🇲	🇿🇲	—	🇿🇲	🇿🇲	🇿🇲	—	—	—	—	flag: Zambia
1739	U+1F1FF U+1F1FC	🇿🇼	🇿🇼	🇿🇼	🇿🇼	—	🇿🇼	🇿🇼	🇿🇼	—	—	—	—	flag: Zimbabwe
subdivision-flag
№	Code	Browser	Appl	Goog	FB	Wind	Twtr	Joy	Sams	GMail	SB	DCM	KDDI	CLDR Short Name
1740	U+1F3F4 U+E0067 U+E0062 U+E0065 U+E006E U+E0067 U+E007F	🏴󠁧󠁢󠁥󠁮󠁧󠁿	🏴󠁧󠁢󠁥󠁮󠁧󠁿	🏴󠁧󠁢󠁥󠁮󠁧󠁿	🏴󠁧󠁢󠁥󠁮󠁧󠁿	—	🏴󠁧󠁢󠁥󠁮󠁧󠁿	🏴󠁧󠁢󠁥󠁮󠁧󠁿	🏴󠁧󠁢󠁥󠁮󠁧󠁿	—	—	—	—	flag: England
1741	U+1F3F4 U+E0067 U+E0062 U+E0073 U+E0063 U+E0074 U+E007F	🏴󠁧󠁢󠁳󠁣󠁴󠁿	🏴󠁧󠁢󠁳󠁣󠁴󠁿	🏴󠁧󠁢󠁳󠁣󠁴󠁿	🏴󠁧󠁢󠁳󠁣󠁴󠁿	—	🏴󠁧󠁢󠁳󠁣󠁴󠁿	🏴󠁧󠁢󠁳󠁣󠁴󠁿	🏴󠁧󠁢󠁳󠁣󠁴󠁿	—	—	—	—	flag: Scotland
1742	U+1F3F4 U+E0067 U+E0062 U+E0077 U+E006C U+E0073 U+E007F	🏴󠁧󠁢󠁷󠁬󠁳󠁿	🏴󠁧󠁢󠁷󠁬󠁳󠁿	🏴󠁧󠁢󠁷󠁬󠁳󠁿	🏴󠁧󠁢󠁷󠁬󠁳󠁿	—	🏴󠁧󠁢󠁷󠁬󠁳󠁿	🏴󠁧󠁢󠁷󠁬󠁳󠁿	🏴󠁧󠁢󠁷󠁬󠁳󠁿	—	—	—	—	flag: Wales

Access to Copyright and terms of use
Last updated:  - 12/14/2019, 4:22:41 AM - Contact Us
"""

all_characters = set(unicode_website)
normal_characters = set(string.printable)
emojis = all_characters - normal_characters

with open(export_path, 'w') as outfile:
    json.dump(list(emojis), outfile)

# Import code.
# import_path = export_path
# with open(import_path) as f:
#     emojis = json.load(f)

print('emoji list:')
print(emojis)

# To use emojis for regex, use:
# emoji_pattern = '|'.join(emojis))