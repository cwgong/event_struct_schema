# -*- coding: utf-8 -*-

print(0*0)

a = [0,1,2]

dic = {}

for x in a :
    if x not in dic.keys():
        dic[x] = 1

print(dic)

tuples = [ ]
tuples.append((dic,[1,2,3]))
tuples.append((dic,[1,2,3]))
tuples.append((dic,[1,2,3]))

print(tuples)

import re

        
def removeAllTag(cls, str):
    if str is None:
        return u""
    dr = re.compile(u'<[^>]+>', re.S)
    dd = dr.sub(u"", str)
    dd = StringUtils.replaceSpecialWords(dd)
    return dd

s = '<d爽肤水></p></p>sd<p><p>fs</p>d<br></p>'
s = re.sub('<[^>]+>','',s)
print(s)