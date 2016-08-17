#!/usr/bin/env python
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import time
import os
import sys

field = sys.argv[1]
year = int(sys.argv[2])
month = int(sys.argv[3])
id = str(year * 100 + month)[2:]
filename = field + '-' + id + '-abs.txt'
print filename,
if os.path.isfile(filename):
	print 'already exists, skip'
	exit()
else:
	print 'fetch'
site = requests.get('http://arxiv.org/list/' + field + '/' + id + '?show=3000')
soup = BeautifulSoup(site.text.encode('ISO-8859-1'))
f = open(filename, 'w')
hrefs = soup.findAll("a", attrs={"title": "Abstract"})
for href in hrefs:
	absUrl = 'http://arxiv.org' + href.get('href')
	absResp = requests.get(absUrl)
	absSoup = BeautifulSoup(absResp.text.encode('ISO-8859-1'))
	abstract = absSoup.find("blockquote")
	if hasattr(abstract, 'contents'):
		f.write(abstract.contents[2].replace('\n', ' ').encode('utf-8'))
	f.write('\n')
f.close()
