# Here's some new strainge stuff, remember type it exactly

# assign various strings to varaibles that we may print out later in the script
days = "Mon Tue Wed Thu Fri Sat Sun"
months = "\nJan\nFeb\nMar\nApr\nMay\nJun\nJul\nAug\nSep\nOct\nNov\nDec"

# we're contactating a string and a variable with a comma, not a plus sign...what does a plus sign do instead?
print("Here are the days: ", days)
# looks like a plus sign does the same thing, the comma is certainly cleaner
print("Here are the days (using a plus sign): " + days)
print("And here are the months: ", months)

# Starting our first line of text on the next line down of this print seems to do the same thing as \n or the escape sequence to create a new line
# NOTE that the triple-quotes have no spaces between them...
print("""
There's something going on here.
With the three double-quotes.
We'll be able to type as much as we like.
Even 4 lines if we want, or 5 or 6!
""")