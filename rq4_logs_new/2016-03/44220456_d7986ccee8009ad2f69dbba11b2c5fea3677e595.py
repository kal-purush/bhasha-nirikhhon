def door_return_refuse():
	print """
	TO RETURN UNCHANGED,
	STILL UNWORTHY,
	IS AN OFFENSE TO DIVINITY.
	TURN BACK UNTIL YOU FIND WORTH
	OR FACE JUDGEMENT UNPREPARED.
	"""

def door_return():
	print """ 
	YOU'VE COME BACK SMALL CREATURE,
	BUT SOMETHING IS DIFFERENT.
	ARE YOU READY FOR JUDGEMENT?
	"""

def door_return_yes():
	print """
	The corners of Yorham's enormous eye
	crinkle into a dark, divine smile.
	YES.
	IT IS TIME.
	GIVE YOURSELF TO ME.
	TO HIM.
	The eye quivers and rolls back into its lid,
	revealing nothingness behind it.
	A strong urge to insert your arm into this new darkness
	arises deep within you.
	Do you succumb to desire?
	"""

def door_return_no():
	print """ 
	TO SUBMIT TO JUDGEMENT IS A FORMALITY.
	HE WILL KNOW YOU.
	The eye rotates violently back into its lid,
	and you find your arm sucked deep within the door.
	Attempts to release yourself only leave you
	pulled tighter in against the stone.
	"""
	#probably just goes straight into  the judgemment bit with no action

def door_judgement():
	print """
	Freezing cold, intense, 
	followed by a sharp, 
	stinging pain that runs up and down your entire arm,
	just along your skin at first,
	but soon after it penetrates,
	and pulses in your muscles,
	in your veins,
	pounds against your bones.
	It is an otherworldy pain,
	larger than your understanding.
	Yorham whispers to you softly 
	as the hurt floods your consciousness,
	AH, YOU HAVE TASTED THE BLOOD. 
	"""
	##maybe make this a choice instead of a straight up print.
	#your body screams at you to do something to get rid of the pain
	#but something inside you forces you to remain

def door_judgement_resist():
	print """
	TO LIVE IN THIS WORLD IS 
	TO DESERVE HIS JUDGEMENT.
	BELIEF IN CHOICE IS SMALL, HUMAN, FOOLISH.
	"""
	if 1=1 door_judgement() ###how do i do this correctly?

def enter_fortress():
	print """ 
	Your entirety tingles 
	as you step onto hallowed ground
	and your stone slams down behind you.
	This is nothing like the 
	primitive clearing you saw earlier, 
	sapphire cobblestones line manicured paths,
	it exudes a grand an ancient quality 
	that makes your mind buzz, 
	and it is difficult for you to take it all in at once.
	Once you have adjusted to glory,
	you can comprehend your surroundings.
	In the distance, the glinting spire 
	you noticed on arrival 
	still shines bright and you 
	notice for the first time that 
	it is in the shape of an enormous spined seashell,
	just like the one engraved above Yorham.
	Closer, a line of white 
	marble buildings with shining metal doors
	labeled one through twelve sit just off the path.
	Next to you, a giant stone sphinx sits. 
	Its face has been destroyed,
	where an expression should sit is just a jagged stump,
	but still you feel as though it is staring at you.
	Do you address the mythic creature?
	Or investigate the marble buildings?
	Or move towards the bright shining shell,
	the source of Taymor's obsession 
	that has beckoned to you since the beginning,
	perhaps even earlier.
	"""

def faceless_sphinx_approach():
	print """ 
	You approach the faceless wonder.
	Instinctually, you kneel before it.
	Do you have a question for the myth?
	Or is its presence enough to satisfy?
	"""

def sphinx_defer
	print """ 
	The stone giant's voice 
	echoes across the walled fortress.
	Without a mouth, it seems to come 
	rumbling out from somewhere deep within its chest.
	Fear and sadness flood your mind, as she purrs to you.
	TO KNEEL BEFORE ME IS HERESY.
	MY FACE IS GONE, BUT THEY HAVE LEFT MY BODY AS A SIGN.
	I HAVE LEARNED A GREATER LESSON,
	AND AM HAPPY PASSING IT ALONG AS SERVICE
	TO THOSE GREATER THAN MYSELF.
	IF YOU HAVE COME TO ASCEND,
	UP QUICK AND ONWARD,
	UNLESS THERE IS SOMETHING YOU WISH TO KNOW FROM ME.
	"""

	#lead in bit - same thing with the rumbling -
	#Any response to the sphinx leads to his awakening, then the relevant bit
	#but the awakening only happens once. not sure exactly how we'll do that but know it's possible

def sphinx_taymor():
	print """ 
	AH, YES.
	IT IS YOU THAT THEY WAIT FOR.
	TO HAVE ENTERED OUR WALLS IS SOME PROOF OF YOUR WORTH.
	WHETHER IT IS JUST YOUR BLOOD THEY NEED,
	OR IF YOU ARE HIDING GREATER VALUE REMAINS TO BE SEEN.
	I HAVE HEARD THE MEN WHO 
	DISFIGURED ME SPEAK OF YOUR ARRIVAL.
	YOU SHARE SOMETHING WITH THE GREAT ONE.
	WILL YOU GIVE YOURSELF OVER 
	WHEN THEY ASK FOR YOUR SACRIFICE?
	ARE YOU PREPARED TO MEET DIVINITY?
	"""
	#this path is for anything to do with taymor, or mention of great power
def sphinx_taymor_yes():
	print """ 
	CARRY ON.
	SOMETHING ENORMOUS AND FINAL AWAITS YOU.
	"""

def sphinx_taymor_no():
	print """ 
	THAT IS UNFORTUNATE.
	SOMETHING IRREVERSIBLE HAS BEEN TRIGGERED.
	FATE AWAITS.
	"""
	#after either of these it feeds back into def fortress_reassess
	
def sphinx_murex():
	print """
	THE SHELL IS DAZZLING,
	AND IT IS NO WONDER IT HAS INSPIRED SUCH LOYALTY.
	BUT THE SYMBOL IS NOTHING 
	COMPARED TO THE POWER IT ANNOUNCES.
	I HAVE FELT IT COMING.
	TO FEAR A GOD IS A HEALTHY AND HUMAN THING.
	TO ATTEMPT TO MEET ONE AS AN EQUAL IS FOOLISH,
	AND MANY SOULS HAVE VANISHED IN THE FACE OF GREATNESS.
	TO RISE UP HIGH ENOUGH TO TOUCH THE STARS
	THEN CHOOSE TO BEND THE KNEE
	IS THE ONLY PATH TO ASCENSION.
	 """
	#mention of shell or the actual words murex or order of
def sphinx_diary():
	print """ 
	HERETICS BURN LIKE RAGS DOUSED IN OIL.
	THEY ARE KINDLING FOR THE GREAT FIRE
	THAT COMES TO LIGHT OUR WAY.
	MONGRELS IN LIFE,
	THE SEPARATISTS WERE GIVEN WORTH IN SACRIFICE.
	THE GREAT ONE REWARDS THEM 
	BY FORGETTING THEIR NAMES FOREVER,
	THEIR SINS WILL NOT STAIN FUTURE GENERATIONS.
	"""
	#this path is anything that mentions the diary, heresy, or Leicester
def sphinx_aggression():
	print """ 
	A deep rumbling shakes the ground beneath your feet.
	After a moment, you recognize its rhythm, 
	and realize that the monster
	is laughing at you.
	IT IS TRUE,
	I HAVE BEEN HUMBLED.
	BUT HUMILIATION BY A GREATER POWER
	HAS NOT MADE A HUMAN ANY MORE FORMIDABLE.
	FURTHER VIOLENCE WILL RESULT IN YOUR EXTINCTION.
	"""

	#any violence or anger
def sphinx_aggression_two():
	print """ 
	I AM CAPABLE OF FORGIVENESS,
	IT IS A CHOICE I MAKE.
	CHANGE YOUR COURSE OR DIE.
	"""

def sphinx_aggression_three():
	print """ 
	You had believed that the statue 
	was unable to move.
	As it leaps from its pose and arcs high above you,
	you realize that you were wrong.
	Belief in complete understanding is dangerous and foolish,
	and today you are punished for it.
	You were meant for greater things,
	but this is not the first time that 
	stupidity has altered destiny. 
	"""

def sphinx_beach():
	print """ 
	A HOLY AND MEMORABLE SITE.
	TO MAKE THE PILGRIMAGE IS A GREAT HONOR
	FOR EVERY LOYAL SERVANT.
	THEY WILL BE PLEASED AT YOUR ENCOUNTER WITH HISTORY.
	"""
	#anything mentioning the tbd rocky beach scene
#he tells you that he's been disfigured by greater powers
#to continue is to look into the face of divinity, not everyone survives
#to press on is necessary, but survival is uncertain;;;;;;;

def fortress_reassess():
	print """ 
	The same options lay before you,
	do you have further questions for the sphinx?
	Would you like to invesitigate the marble structures?
	Or do you press on towards the pyramid? 
	"""

def sphinx_return():
	#after first round of talking
	print """ 
	GREATNESS OR DESTRUCTION AWAIT.
	I AM OF NO FURTHER CONSEQUENCE.
	BE GONE AND LET THE STRENGTH OF YOUR BLOOD BE WEIGHED
	AGAINST REAL POWER.
	LEARN THE WORTH OF YOUR DESTINY. 
	"""
	#feeds straight back into fortress_reassess

def barracks_investigate():
	print """ 
	The row of marble structures appear 
	to be completely 
	identical aside from the numbers 
	ascending from one to twelve.
	Which do you decide to enter? 
	"""
#if you decide to investigate barracks, you are asked which number you choose, 
	#outcome is decided on whether you select an even or odd number
def barracks_odd():
	print """ 
	The polished copper door swings open 
	easily when you push it.
	Inside, the floor and walls are 
	made of the same clean white marble as the facades.
	There is no electric light, but the entire room is perfectly
	illuminated by one circular window, 
	set strangely into a corner of the ceiling at an angle.
	A small single bed stands neatly made against one wall.
	Beside it, a small nightstand 
	with a book bound in purple leather resting on top.
	One wooden chair sits in the center of the room, 
	facing a wall.
	In the corner, a chamber pot of the same glistening copper
	emits a stench that makes it impossible to believe 
	another human has been here recently.
	"""
#this could be a nice place to have a look around reward
def odd_chair():
	print """ 
	The purpose of this stiff-backed chair is
	immediately revealed as you take a seat.
	It faces the odd window,
	which functions both as a skylight
	and a perfect frame 
	for the shining seashell on top of the pyramid
	at the end of the compound.
	It is truly dazzling,
	and you have a hard time pulling your eyes away.
	"""
def odd_under_bed():
	print """ 
	A thin layer of white dust coats your clothes
	as you get down to peer underneath the bed.
	But otherwise it is spotless.
	An image of a dull and serious man,
	perhaps robed,
	crosses your mind.
	
	And then, immediately after that,
	a memory of the yard behind your aunt's home one winter,
	where a pond had frozen through solid,
	and you could see a fish,
	stuck up near the surface
	with large eyes that conveyed a dense, stupid, animal
	horror to anyone who looked down at it until the thaw.

	These images are perhaps unrelated,
	but they do cross your mind,
	and occasionally that means something. 
	"""

def odd_book():
	print """ 
	Embossed on the soft leather cover is the title
	HANDBOOK OF THE ORDER OF THE MUREX
	FOR SERVANTS' EYES ONLY
	Do you put it back or look inside?
	"""

def odd_book_read():
	print """ 
	The first page reads:

	Welcome servant,
	and congratulations.
	The Murex tells the coming of the Great One.
	Soon, He will arrive 
	ready to illuminate, to lift up, to consume.

	You have begun the march to ascension.
	The text of this book after this page 
	is written in an alphabet and language 
	that predates Earth, this dimension, 
	and any idea of divinity your 
	ancestors were familiar with.
	You will not be expected to read it,
	but you must carry it with you at all times
	and understand that every rule and command
	passed down by myself,
	your High Priest,
	comes directly from this holy text,
	and as such,
	is also a command from the One We Wait For.

	Work hard, toil for Him.
	Embrace the peace that comes with it,
	and dream of the greatness that awaits you beyond.

	High Priest Taymor Huntingdon VII

	Future pages are indeed in a looping, character-based
	alphabet that you do not understand or even recognize.
	You wonder if your great-grandfather could actually read
	this, and if so, how he learned.
	"""

def odd_window():
	pass

def odd_bed():
	print """ 
	The mattress is stiff and the sheets are coarse.
	Whoever slept here did not enjoy it.
	There is a hard and Spartan feeling 
	left behind in this room
	along with the smell. 
	"""

def odd_chamber_pot():
	print """ As your senses led you to suspect,
	this pot is full of excrement.
	"""

def barracks_even():
	print """
	The polished copper door pushes a purple cotton robe
	across the floor as you open it.
	A single, mussed bed stands against one wall.
	An overturned chair is the only other furniture in the room.
	There is no electric light, but the entire room is perfectly
	illuminated by one circular window, 
	set strangely into a corner of the ceiling at an angle.
	"""
	#similar, but the bed is mussed. book can be found under the bed. chair is knocked over.
	#if search, glint coming from underneath bed

def even_upright_chair():
	print """ 
	You set the chair upright in the center of the room.
	There is no desk,
	nor would there be room for much of one.
	It is unclear what the chair was meant to accomplish 
	that the bed could not.
	"""

def even_bed():
	print """ 
	You instinctually move to straighten 
	the coarse sheets on the little bed,
	until you realize that they are 
	giving off the musty smell of stale urine. 
	Everything has felt disorienting 
	and occasionally frightening
	since you received the letter from Taymor,
	but something about this room 
	just makes you want to sit down and cry.
	"""

def even_chamber_pot():
	print """ 
	You follow the glint to look under the bed and 
	find a shining copper chamber pot overturned underneath.
	It contains a bloody rag.
	You recoil and stand up straight 
	before giving in to the bodily urge to vomit.
	"""

def leave_barracks():
	pass
#feeds straight back to reassess

def pyramid_enter():
	pass

def dobbins_encounter():
	pass

def pyramid_survey():
	pass

def taymor_speech():
	pass

def ritual_begin():
	pass

def ritual_interrupt():
	pass