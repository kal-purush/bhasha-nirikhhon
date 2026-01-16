import {Name} from './types';

/**
1. Typical list of 3+
  'David Mimno, Hanna M Wallach, and Andrew McCallum' ->
    ['David Mimno', 'Hanna M Wallach', 'Andrew McCallum']
2. List of 3+ without the Oxford comma, in case that ever happens
  'Aravind K Joshi, Ben King and Steven Abney' ->
    ['David Mimno', 'Hanna M Wallach', 'Andrew McCallum']
3. Duo
  'Daniel Ramage and Chris Callison-Burch' ->
    ['David Mimno', 'Chris Callison-Burch']
4. Single author
  'David Sankofl' ->
    ['David Sankofl']
5. Et al. abbreviation
  'Zhao et al.' ->
    ['Zhao', 'et al.']
*/
export function splitNames(input: string): string[] {
  // three split options: (, and ) or ( and ) or (, )
  // TODO: fix the 'et al.' hack
  return input.replace(/\s+et al\./, ', et al.').split(/,\s*and\s+|\s*and\s+|,\s*/);
}

/**
Typically, in-paper citations (`Cite`s) only have the last names of the authors,
while the `Reference`s in the Bibliography have full names, or at least first
initials and last names.

This method determines whether a `Cite`'s names match a `Reference`'s authors.

    authorsMatch(['Joshi'], ['Aravind K Joshi']) -> true
    authorsMatch(['Diab', 'Kamboj'], ['Mona Diab', 'Ankit Kamboj']) -> true

'et al.' gets special treatment. 'et al.' is a match if and only if there are
more reference authors beyond the one parallel to the 'et al.' citation author.
In other words, 'et al.' cannot stand in for a single author.

    authorsMatch(['Blei', 'et al.'], ['David M Blei', 'Andrew Y Ng', 'Michael I Jordan']) -> true
*/
export function authorsMatch(citeAuthors: Name[], referenceAuthors: Name[]) {
  return citeAuthors.every((citeAuthor, i) => {
    if (citeAuthor.last === referenceAuthors[i].last) {
      return true;
    }
    if (citeAuthor.last === 'et al.' && referenceAuthors.length > (i + 1)) {
      return true;
    }
    return false;
  });
}

/**
Given a name represented by a single string, parse it into first name, middle
name, and last name.

parseAuthor('Leonardo da Vinci') -> { first: 'Leonardo', last: 'da Vinci' }
parseAuthor('Chris Callison-Burch') -> { first: 'Chris', last: 'Callison-Burch' }
parseAuthor('Hanna M Wallach') -> { first: 'Hanna', middle: 'M', last: 'Wallach' }
parseAuthor('Zhou') -> { last: 'Zhou' }
parseAuthor('McCallum, Andrew') -> { first: 'Andrew', last: 'McCallum' }
*/
export function parseName(input: string): Name {
  // 0. 'et al.' is a special case
  if (input === 'et al.') {
    return {last: input};
  }
  // 1. normalize the comma out
  input = input.split(/,\s*/).reverse().join(' ');
  // 2. split on whitespace
  var parts = input.split(/\s+/);
  var n = parts.length;
  if (n >= 3) {
    return {
      first: parts[0],
      middle: parts.slice(1, n - 1).join(' '),
      last: parts[n - 1],
    };
  }
  else if (n == 2) {
    return {
      first: parts[0],
      last: parts[1],
    };
  }
  return {
    last: parts[0]
  };
}