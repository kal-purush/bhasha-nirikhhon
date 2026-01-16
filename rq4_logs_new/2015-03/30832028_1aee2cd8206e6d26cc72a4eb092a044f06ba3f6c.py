#!-*- coding: utf-8 -*-

import prefixverb

pv = prefixverb.prefixverben(u'test_conf/conf.json')
pv.setPVcol()
res = pv.getDataFromCorpus()
pv.saveDataToSnapshot(res)