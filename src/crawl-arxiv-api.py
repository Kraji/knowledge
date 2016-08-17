#!/usr/bin/env python
import os
import sys
import urllib
from xml.dom import minidom

argc = len(sys.argv)
if (argc == 1):
  print 'USAGE: {} [catogory] [start] [max_results]'.format(sys.argv[0])
  exit(1)

cat = sys.argv[1]
start = 0
max_results = 1000
if (argc > 2):
  start = int(sys.argv[2])
if (argc > 3):
  max_results = int(sys.argv[3])
print 'catogory = {}\nstart = {}\nmax_results = {}'.format(cat, start, max_results)

url = 'http://export.arxiv.org/api/query?search_query=cat:{}&start={}&max_results={}'.format(cat, start, max_results)
print url
data = urllib.urlopen(url).read()
xmldoc = minidom.parseString(data)

files = {}
entries = xmldoc.getElementsByTagName('entry') 
for entry in entries:
  idUrl = entry.getElementsByTagName('id')[0].childNodes[0].data
  tokens = idUrl.split('/')
  id = tokens[len(tokens) - 1]
  title = entry.getElementsByTagName('title')[0].childNodes[0].data.encode('ascii', 'ignore').replace('\n', ' ')
  abstract = entry.getElementsByTagName('summary')[0].childNodes[0].data.encode('ascii', 'ignore').replace('\n', ' ').lstrip()
  year = str(id[0:2])
  if (files.has_key(year) == False):
    filename = cat + '-' + year + '.txt'
    print 'created file ' + filename
    files[year] = open(filename, 'a')
  files[year].write(id + '\n' + title + '\n' + abstract + '\n\n')

for key in files:
  files[key].close()
