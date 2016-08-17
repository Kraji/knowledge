import time
import urllib2
import datetime
from itertools import ifilter
from collections import Counter, defaultdict
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import matplotlib.pylab as plt
import pandas as pd
import numpy as np
import bibtexparser
from tqdm import tqdm

pd.set_option('mode.chained_assignment','warn')

OAI = "{http://www.openarchives.org/OAI/2.0/}"
ARXIV = "{http://arxiv.org/OAI/arXiv/}"

topic = "physics:hep-ex"

def harvest(arxiv="physics:hep-ex"):
    df = pd.DataFrame(columns=("title", "abstract", "categories", "created", "id", "doi"))
    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    url = (base_url +
           "from=2014-12-01&until=2014-12-31&" +
           "metadataPrefix=arXiv&set=%s"%arxiv)
    
    while True:
        print "fetching", url
        try:
            response = urllib2.urlopen(url)
            
        except urllib2.HTTPError, e:
            if e.code == 503:
                to = int(e.hdrs.get("retry-after", 30))
                print "Got 503. Retrying after {0:d} seconds.".format(to)

                time.sleep(to)
                continue
                
            else:
                raise
            
        xml = response.read()

        root = ET.fromstring(xml)

        for record in tqdm(root.find(OAI+'ListRecords').findall(OAI+"record")):
            arxiv_id = record.find(OAI+'header').find(OAI+'identifier')
            meta = record.find(OAI+'metadata')
            info = meta.find(ARXIV+"arXiv")
            created = info.find(ARXIV+"created").text
            created = datetime.datetime.strptime(created, "%Y-%m-%d")
            categories = info.find(ARXIV+"categories").text

            # if there is more than one DOI use the first one
            # often the second one (if it exists at all) refers
            # to an eratum or similar
            doi = info.find(ARXIV+"doi")
            if doi is not None:
                doi = doi.text.split()[0]
                
            contents = {'title': info.find(ARXIV+"title").text,
                        'id': info.find(ARXIV+"id").text,#arxiv_id.text[4:],
                        'abstra#ct': info.find(ARXIV+"abstract").text.strip(),
                        'created': created,
                        'categories': categories.split(),
                        'doi': doi,
                        }

            df = df.append(contents, ignore_index=True)

        # The list of articles returned by the API comes in chunks of
        # 1000 articles. The presence of a resumptionToken tells us that
        # there is more to be fetched.
        token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
        if token is None or token.text is None:
            break

        else:
            url = base_url + "resumptionToken=%s"%(token.text)
            
    return df

    
df = harvest()
df.head()

def get_cites(arxiv_id):
    cites = []
    base_url = "http://inspirehep.net/search?p=refersto:%s&of=hx&rg=250&jrec=%i"
    offset = 1
    
    while True:
        print base_url%(arxiv_id, offset)
        response = urllib2.urlopen(base_url%(arxiv_id, offset))
        xml = response.read()
        soup = BeautifulSoup(xml)

        refs = "\n".join(cite.get_text() for cite in soup.findAll("pre"))

        bib_database = bibtexparser.loads(refs)
        if bib_database.entries:
            cites += bib_database.entries
            offset += 250
            
        else:
            break

    return cites

step = 1000
for N in range(0,17):
    print N
    cites = df['id'][N*step:(N+1)*step].map(get_cites)
    df.ix[N*step:(N+1)*step -1,'cited_by'] = cites

df['citation_count'] = df.cited_by.map(len)

store = pd.HDFStore('harvest.h5')
store['df'] = df
df = store['df']
store.close()
