import requests
from bs4 import BeautifulSoup
from collections import namedtuple
from tabulate import tabulate
import re
import argparse
import unidecode
import sys

Tense = namedtuple('Tense', ['mode', 'tense', 'conjugations'])
Conjugation = namedtuple('Conjugation', ['person', 'form'])

class NouvelObs:
  def __init__(self, url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    self.tenses = [self.create_tense(t) for t in soup.find_all("div", attrs={"class": "tempstab"})]

  def create_conjugation(self, mode, index, br):
    form = ''.join([
      e.text if type(e).__name__ == 'Tag' and e.name == 'b'
      else str(e) if type(e).__name__ == 'NavigableString'
      else ''
      for e in [br.previous_sibling.previous_sibling, br.previous_sibling]
    ]).strip()
    if form == '-':
      return None
    return Conjugation(
      person = self.person(mode, index, form),
      form = form
    )

  def create_tense(self, t):
    mode = re.sub('\\s\\(.*?\\)', '', t.find_previous_sibling("h2", attrs={"class": "mode"}).text.lower().strip())
    return Tense(
      mode = mode,
      tense = re.sub('\\s\\(.*?\\)', '', t.find("h3").text.lower().strip()),
      conjugations = [c for c in [self.create_conjugation(mode, i, br) for i, br in enumerate(t.find("div", attrs={"class": "tempscorps"}).find_all("br"))] if c is not None]
    )

  def query(self, mode, tense, person):
    tenses = [t for t in self.tenses if
      unidecode.unidecode(t.mode).lower() == unidecode.unidecode(mode).lower() and
      unidecode.unidecode(t.tense).lower() == unidecode.unidecode(tense).lower()
    ]
    if tenses:
      return [c.form for c in tenses[0].conjugations if
        person == None or
        unidecode.unidecode(c.person).lower() == unidecode.unidecode(person).lower()
      ]
    else:
      return None

class French(NouvelObs):
  language = 'fr'

  def __init__(self, verb):
    if verb is None:
      verb = 'avoir'
    super().__init__('https://la-conjugaison.nouvelobs.com/du/verbe/%s.php' % verb)

  def person(self, mode, index, form):
    if re.search("^(je\\s|j'|que je\\s|que j')", form):
      return "je"
    if re.search("^(tu|que tu)\\s", form):
      return "tu"
    if re.search("^(il|qu'il)\\s", form):
      return "il/elle"
    if re.search("^(nous|que nous)\\s", form):
      return "nous"
    if re.search("^(vous|que vous)\\s", form):
      return "vous"
    if re.search("^(ils|qu'ils)\\s", form):
      return "ils/elles"
    if mode == 'impératif':
      return ['tu', 'nous', 'vous'][index]
    return None

class Portuguese(NouvelObs):
  language = 'pt'

  def __init__(self, verb):
    if verb is None:
      verb = 'haver'
    super().__init__('https://la-conjugaison.nouvelobs.com/portugais/verbe/%s.php' % verb)

  def person(self, mode, index, form):
    if re.search("^(eu|se eu|quando eu)\\s", form):
      return "eu"
    if re.search("^(tu|se tu|quando tu)\\s", form):
      return "tu"
    if re.search("^(ele|se ele|quando ele)\\s", form):
      return "ele/ela"
    if re.search("^(nós|se nós|quando nós)\\s", form):
      return "nós"
    if re.search("^(vós|se vós|quando vós)\\s", form):
      return "vós"
    if re.search("^(eles|se eles|quando eles)\\s", form):
      return "eles/elas"
    if mode == 'imperativo':
      return [None, 'tu', 'ele/ela', 'nós', 'vós', 'eles/elas'][index]
    return None

class Spanish(NouvelObs):
  language = 'es'

  def __init__(self, verb):
    if verb is None:
      verb = 'haber'
    super().__init__('https://la-conjugaison.nouvelobs.com/espagnol/verbe/%s.php' % verb)

  def person(self, mode, index, form):
    if re.search("^(yo)\\s", form):
      return "yo"
    if re.search("^(tú)\\s", form):
      return "tú"
    if re.search("^(él)\\s", form):
      return "él/ella"
    if re.search("^(nosotros)\\s", form):
      return "nosotros"
    if re.search("^(vosotros)\\s", form):
      return "vosotros"
    if re.search("^(ellos)\\s", form):
      return "ellos/ellas"
    if mode == 'imperativo':
      return [None, 'tú', 'él/ella', 'nosotros', 'vosotros', 'ellos/ellas'][index]
    return None

class Italian(NouvelObs):
  language = 'it'

  def __init__(self, verb):
    if verb is None:
      verb = 'avere'
    super().__init__('https://la-conjugaison.nouvelobs.com/italien/verbe/%s.php' % verb)

  def person(self, mode, index, form):
    if re.search("^(io|che io)\\s", form):
      return "io"
    if re.search("^(tu|che tu)\\s", form):
      return "tu"
    if re.search("^(lui|che lui)\\s", form):
      return "lui/lei"
    if re.search("^(noi|che noi)", form):
      return "noi"
    if re.search("^(voi|che voi)", form):
      return "voi"
    if re.search("^(loro|che loro)", form):
      return "loro/lora"
    if mode == 'imperativo':
      return [None, 'tu', 'lui/lei', 'noi', 'voi', 'loro/lora'][index]
    return None

class English(NouvelObs):
  language = 'en'

  def __init__(self, verb):
    if verb is None:
      verb = 'have'
    super().__init__('https://la-conjugaison.nouvelobs.com/anglais/verbe/%s.php' % verb)

  def person(self, mode, index, form):
    if re.search("^(I)\\s", form):
      return "I"
    if re.search("^(you)\\s", form) and index == 1:
      return "you"
    if re.search("^(he)\\s", form):
      return "he/she"
    if re.search("^(we)", form):
      return "we"
    if re.search("^(you)", form) and index == 4:
      return "you"
    if re.search("^(they)", form):
      return "they"
    return None

### CLI ###

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Conjugate verbs.')
  parser.add_argument('language', help='select the language [%s]' % ', '.join([l.language for l in NouvelObs.__subclasses__()]))
  parser.add_argument('verb', nargs='?', default=None, help='optional or show list of tenses')
  parser.add_argument('mode', nargs='?', default=None)
  parser.add_argument('tense', nargs='?', default=None, help='use quotes if multiple words')
  parser.add_argument('person', nargs='?', default=None, help='optional or show all persons')
  args = parser.parse_args()

  if args.verb:
    for arg in ['mode', 'tense']:
      if not args.__dict__[arg]:
        sys.exit('missing %s' % arg)

  language = [c(args.verb) for c in NouvelObs.__subclasses__() if c.language == args.language]
  if not language:
    sys.exit('unknown language %s' % args.language)
  if args.verb is None:
    [print(t.mode, t.tense) for t in language[0].tenses]
  else:
    print('\n'.join(language[0].query(args.mode, args.tense, args.person)))