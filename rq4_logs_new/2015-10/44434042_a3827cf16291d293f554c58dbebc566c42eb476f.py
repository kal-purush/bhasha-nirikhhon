#IMPLEMENTATION OF LINEAR SEARCH
#IF YOU ARE PASSING A USER-DEFINED OBJECT, MAKE SURE THE CLASS HAS THE OPERATOR '==' OVERLOADED IN ORDER FOR THE COMPARISON TO WORK. YOU CAN DO THIS AS FOLLOWING INSIDE THE CLASS:
#def __eq__ (self, otherObject):
	#LOGIC TO DETERMINE WHETHER 2 OBJECTS ARE EQUAL OR NOT

def get_input ():
	#THE LOGIC FOR HOW YOU WISH TO TAKE THE INPUT AND IN WHAT FORM GOES HERE. REPLACE pass; WITH YOUR CODE
	pass;

def get_key ():
	#INPUT FOR THE ELEMENT TO SEARCH FOR. REPLACE pass; WITH YOUR CODE
	pass;

def linear_search (data, key):
	position = -1;
	for element in data:
		position += 1;
		if (element == key):
			return (position);
	return (-1);

def main ():
	data = get_input ();
	key = get_key ();

	#FINAL OUTPUT STORED IN keyPosition
	#-1 means the key doesn't exist in the data
	keyPosition = linear_search (data, key);

	#PYTHON, HOWEVER, PROVIDES A MUCH MORE SLEEK METHOD FOR SEARCH. STILL, TO DEMONSTRATE THE ALGORITHMS IS IMPORTANT.
	#keyPosition = data.index (key);

if (__name__) == '__main__':
	main ();