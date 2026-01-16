# PyDbLite, statement of purpose:
#   To provide support for the Entity system by tracking entities,
#   providing search functions, and save/pull data from disk.
#   Each PyDbLite database only tracks one table at once, so
#   this cuts out the "relational" part that would be useful.
#   It may not be needed since you can make cohesion-ware
#   where the time-cost is cheap. However, if anything starts to
#   become too convoluted because you have to create too much
#   to bridge that gap, then move to another DB library.

import pydblite

from pydblite import Base

# seems that only one field per db
db = Base('test.pdl')                       #creates database/single-table
db.create('name','age','size',mode='override')  #creates fields
# mode='open', opens if the db exists and creates if not
# mode='override', overides existing db with new one, use for testing
# implicit __id__ value included to ensure primary_key

#both insert
db.insert(name='billy',age=17,size=1.5) #by keywords
db.insert('elegia',83,2.1)              #by position
db.insert('jeremy',702,0.2)

db.commit() #FUCKIN IMPORTANT DONT FORGET!!!!!!!!!!!!!!!! <----------------
# commits changes
# using open() to pull values from dick, nullifying uncommited changes

#iterate over all records
def fp(): #full print
    for r in db:
        print(r)
    print('#######')
fp()

#direct access to entry with id -> rec_id
record = db[0]  #check: record['__id__'] = rec_id

#index field
db.create_index('age')

#db search supports built-in comparison functions and return lists
for r in db('age') >= 20:
    print(r)
print('#######')
#db support list comprehension
really_really_really_short = next(r for r in db('size') < 1.0)

#update supports record(s) value(s) and updates the indicie
db.update(really_really_really_short,size=0.1) ; fp()       #even shorter
db.update(db,age='23') ; fp()

#delete supports single and multiple records
db.delete( r for r in db('size') >= 0.2 ) ; fp()
del db[next( r for r in db('size') < 0.2 )] ; fp()


#useful utility functions
db.add_field('mood',default='catcucumber')  # adds field, with optional default value
db.drop_field('mood')                       # drops field
db.path                                     # path of db, can be changed
db.name                                     # name of db, stripped of path
db.fields                                   # fields of db, excludes __id__ & __version__
len(db)                                     # number of records in db