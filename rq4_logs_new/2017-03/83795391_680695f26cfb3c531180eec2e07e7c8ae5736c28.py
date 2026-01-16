#!/usr/bin/env python
# -*- coding: utf8 -*-

astr='abc'
alist=['hello','world']
atuple=(10,20,30)
adict={'name':'bob','age':23}
aset=set(['how','are','you'])
fname='/etc/hosts'
aiter=iter(['first','second','third'])

for ch in astr:
    print ch

for word in alist:
    print word

for i in atuple:
    print i

for key in adict:
    print '%s:%s' % (key,adict[key])

for item in aset:
    print item

for element in aiter:
    print element

fobj=open(fname)

for line in fobj:
    print line

fobj.close()