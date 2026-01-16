"""compre logic for html files

output naar hoofdprogramma: list van 3-tuples
1e waarde: node (element, attribuut, tekst)
2e waarde: entry in linkerfile
3e waarde: entry in rechterfile
"""

import sys
import bs4 as bs
import pprint
ELSTART = '<> '
CMSTART = '<!> '

# hoofdroutine: compare_htmldata - kan misschien overgenomen worden uit xml vergelijking en dan
# gefinetuned

# lezen html bestand - m.b.v. BeautifulSoup - generator die afdalend in de boomstructuur
# - in volgorde van het document de elementen opsomt die erin voorkomen (alleen de tagnaam)
# - de bijbehorende attributen sorteert op naam en naam + waarde teruggeeft
# - de gevonden string of strings toont in volgorde van het document


def compare_htmldata(fn1, fn2):
    """compare two similar html documents
    """
    result = []
    gen1 = (x for x in get_htmldata(fn1))
    gen2 = (x for x in get_htmldata(fn2))

    def gen_next(gen):
        "generator to get next values from data collection"
        eof = False
        try:
            elem, attr, val = next(gen)
        except StopIteration:
            eof = True
            elem = attr = val = ''
        return eof, elem, attr, val
    eof_gen1, elem1, attr1, val1 = gen_next(gen1)
    eof_gen2, elem2, attr2, val2 = gen_next(gen2)
    while True:
        if eof_gen1 and eof_gen2:
            break
        get_from_1 = get_from_2 = False
        ## print((sect1, opt1), (sect2, opt2))
        # probleem:dit werkt niet als de elem lijsten ongelijk van lengte zijn want dan
        # is de langste eigenlijk de "kleinste"
        if (eof_gen1, elem1, attr1) < (eof_gen2, elem2, attr2):
            result.append(((elem1, attr1), val1, ''))
            if not eof_gen1:
                get_from_1 = True
        elif (eof_gen1, elem1, attr1) > (eof_gen2, elem2, attr2):
            result.append(((elem2, attr2), '', val2))
            if not eof_gen2:
                get_from_2 = True
        else:
            if (eof_gen1, elem1) < (eof_gen2, elem2):
                result.append(((elem1, attr1), val1, ''))
                if not eof_gen1:
                    get_from_1 = True
            elif (eof_gen1, elem1) > (eof_gen2, elem2):
                result.append(((elem2, attr2), '', val2))
                if not eof_gen2:
                    get_from_2 = True
            else:
                if attr1 < attr2:
                    result.append(((elem1, attr1), val1, ''))
                    get_from_1 = True
                elif attr1 > attr2:
                    result.append(((elem2, attr2), '', val2))
                    get_from_2 = True
                else:
                    result.append(((elem1, attr1), val1, val2))
                    get_from_1 = True
                    get_from_2 = True
        if get_from_1:
            eof_gen1, elem1, attr1, val1 = gen_next(gen1)
        if get_from_2:
            eof_gen2, elem2, attr2, val2 = gen_next(gen2)
    return result


def get_htmldata(filename):
    """de volgorde van elementen en teksten moet intact blijven
    attributen van een element moeten wel gesorteerd worden
    """
    with open(filename) as _in:
        soup = bs.BeautifulSoup(_in, 'lxml')
    return get_next_level_data(soup)


def get_next_level_data(element, level=0):
    "read data under an element"
    result = []
    for item in element.children:
        if isinstance(item, bs.Tag):
            element_tag = (level, ELSTART + item.name)
            result.append([element_tag, '', ''])
            if item.attrs is not None:
                # eerst even ongesorteerd
                for name in sorted(item.attrs):
                    value_list = item.get_attribute_list(name)
                    for value in sorted(value_list):
                        result.append([element_tag, name, value])
            result.extend(get_next_level_data(item, level + 1))
        elif isinstance(item, bs.Comment):
            result.append([(level, ''), '', CMSTART + item.string])
        elif isinstance(item, bs.NavigableString):
            result.append([(level, ''), '', item.string])
        else:
            result.append(["Oops, what's this?" + str(item), '', ''])
    return result


if __name__ == "__main__":
    "read a document and show results"
    with open('html_comp_left', 'w') as out:
        pprint.pprint(get_htmldata(sys.argv[1]), stream=out)
    with open('html_comp_right', 'w') as out:
        pprint.pprint(get_htmldata(sys.argv[2]), stream=out)
    with open('html_comp_both', 'w') as out:
        pprint.pprint(compare_htmldata(sys.argv[1], sys.argv[2]), stream=out)
