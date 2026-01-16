import requests
from bs4 import BeautifulSoup
from collections import namedtuple
from tabulate import tabulate
import re
import argparse
import unidecode

parser = argparse.ArgumentParser(description='Conjugate verbs.')
parser.add_argument('-l', '--list', action='store_true', help='list all modes and tenses')
subparsers = parser.add_subparsers()
parser_query = subparsers.add_parser('query', help='make a conjugation query')
parser_query.add_argument('verb')
parser_query.add_argument('mode')
parser_query.add_argument('tense', help='use quotes if multiple words')
parser_query.add_argument('person', nargs='?', default=None, help='optional')
args = parser.parse_args()

if args.list:
  args.verb = 'aller'

Tense = namedtuple('Tense', ['mode', 'tense', 'conjugations'])
Conjugation = namedtuple('Conjugation', ['person', 'form'])

url = 'https://la-conjugaison.nouvelobs.com/du/verbe/%s.php' % args.verb
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

def person(mode, index, prefix):
  if re.search("^(je|j'|que je|que j')", prefix):
    return "je"
  if re.search("^(tu|que tu)", prefix):
    return "tu"
  if re.search("^(il|qu'il)\\s", prefix):
    return "il/elle"
  if re.search("^(nous|que nous)", prefix):
    return "nous"
  if re.search("^(vous|que vous)", prefix):
    return "vous"
  if re.search("^(ils|qu'ils)", prefix):
    return "ils/elles"
  if mode == 'impératif':
    return ['tu', 'nous', 'vous'][index]
  return None

def prefix(conjugation):
  return str(conjugation.previous_element) if type(conjugation.previous_element).__name__ == 'NavigableString' else ''

def create_tense(t):
  mode = t.find_previous_sibling("h2", attrs={"class": "mode"}).text.lower().strip()
  return Tense(
    mode = mode,
    tense = t.find("h3").text.lower().strip(),
    conjugations = [Conjugation(
      person = person(mode, i, prefix(c)),
      form = (prefix(c) + c.text).strip()
    ) for i, c in enumerate(t.find("div", attrs={"class": "tempscorps"}).find_all("b"))]
  )

tenses = [create_tense(t) for t in soup.find_all("div", attrs={"class": "tempstab"})]

if args.list:
  [print(t.mode, t.tense) for t in tenses]

else:
  query = [t for t in tenses if unidecode.unidecode(t.mode).lower() == unidecode.unidecode(args.mode).lower() and unidecode.unidecode(t.tense).lower() == unidecode.unidecode(args.tense).lower()]
  if query:
    tense = query[0]
    forms = [c.form for c in tense.conjugations if args.person == None or unidecode.unidecode(c.person).lower() == unidecode.unidecode(args.person).lower()]
    print(', '.join(forms))