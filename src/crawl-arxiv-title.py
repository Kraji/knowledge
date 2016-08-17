#!/usr/bin/env python
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import time
import os

for year in range(1992, 2014):
	for month in range(1, 13):
		id = str(year * 100 + month)[2:]
		print id,
		if os.path.isfile(id + '.txt'):
			print 'already exists, skip'
			continue
		else:
			print 'fetch'
		site = requests.get('http://arxiv.org/list/cond-mat/' + id + '?show=3000')
		soup = BeautifulSoup(site.text.encode('ISO-8859-1'))
		f = open(id + '.txt', 'w')
		divs = soup.findAll('div', 'list-title')
		for div in divs:
		    f.write(div.contents[2].encode('utf-8'))
		f.close()
