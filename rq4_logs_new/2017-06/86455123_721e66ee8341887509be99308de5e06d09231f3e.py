#-*- coding: utf-8 -*-
# Copyright 2017 ibelie, Chen Jie, Joungtao. All rights reserved.
# Use of this source code is governed by The MIT License
# that can be found in the LICENSE file.

def test_dict():
	import _typy
	import test_case
	test_case.setup(globals(), _typy)
	global _Fighter
	f = _Fighter()

	BEHAVIOR_SET = 0
	BEHAVIOR_DEL = 1
	BEHAVIOR_ALL = (BEHAVIOR_SET, BEHAVIOR_SET, BEHAVIOR_SET)

	import random
	while 1:
		i = 0
		while i < 10000:
			i += 1
			if len(f.bd) > 10000000:
				bk = BEHAVIOR_DEL
			elif not f.bd:
				bk = BEHAVIOR_SET
			else:
				bk = random.choice(BEHAVIOR_ALL)
			if bk == BEHAVIOR_SET:
				bdk = ''.join([chr(random.randint(ord('a'), ord('z'))) for _ in xrange(random.randint(10, 50))])
				bdv = ''.join([chr(random.randint(ord('a'), ord('z'))) for _ in xrange(random.randint(10, 50))])
				f.bd[bdk] = bdv
				assert f.bd[bdk] == bdv, 'bd %s %s %s' % (bdk, f.bd[bdk], bdv)
			else:
				bdk = random.choice(f.bd.keys())
				del f.bd[bdk]
				assert bdk not in f.bd, 'bd %s %s' % (bdk, f.bd[bdk])

			if len(f.sd) > 10000000:
				sk = BEHAVIOR_DEL
			elif not f.sd:
				sk = BEHAVIOR_SET
			else:
				sk = random.choice(BEHAVIOR_ALL)
			if sk == BEHAVIOR_SET:
				sdk = random.randint(0, 1000000)
				sdv = ''.join([chr(random.randint(ord('a'), ord('z'))) for _ in xrange(random.randint(10, 50))])
				f.sd[sdk] = sdv
				assert f.sd[sdk] == sdv, 'sd %s %s %s' % (sdk, f.sd[sdk], sdv)
			else:
				sdk = random.choice(f.sd.keys())
				del f.sd[sdk]
				assert sdk not in f.sd, 'sd %s %s' % (sdk, f.sd[sdk])

		print len(f.bd), len(f.sd)