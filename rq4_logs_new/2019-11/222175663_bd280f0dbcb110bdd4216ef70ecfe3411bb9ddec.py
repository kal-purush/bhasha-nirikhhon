import dataset

DB = dataset.connect('sqlite:///.german.db')
VERBS = DB.get_table('verb')

eln_verbs = VERBS.find(type='de-conj-weak-eln')

for verb in eln_verbs:
    verb['past_participle'] = "ge{0}elt".format(verb['present_stem'])
    VERBS.update(verb, ['id'])