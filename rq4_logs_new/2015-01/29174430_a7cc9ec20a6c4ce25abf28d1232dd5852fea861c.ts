interface ScoreMatrixRow {
    [letter: number]: number
}

interface ScoreMatrix {
    [letter: number]: ScoreMatrixRow
}

interface QueryWord {
    queryOffset: number
    wordText: string
}

interface Alignment {
    queryOffset: number
    dbOffset: number
    length: number
}

// Return k letter substrings of query
function queryWords(query: string, k: number): Array<QueryWord> {
    return _.map(_.range(0, query.length-k+1), (i: number) => {
        return {
            queryOffset: i,
            wordText: query.slice(i, i+k),
        }
    });
}

// http://prowl.rockefeller.edu/aainfo/pam250.htm
// Hash lookup table with pam250 values. pam250['A']['R'] = 3
function pam250(): ScoreMatrix {
    var lines = [
        '2',
        '-2	6',
        '0	0	2',
        '0	-1	2	4',
        '-2	-4	-4	-5	4',
        '0	1	1	2	-5	4',
        '0	-1	1	3	-5	2	4',
        '1	-3	0	1	-3	-1	0	5',
        '-1	2	2	1	-3	3	1	-2	6',
        '-1	-2	-2	-2	-2	-2	-2	-3	-2	5',
        '-2	-3	-3	-4	-6	-2	-3	-4	-2	2	6',
        '-1	3	1	0	-5	1	0	-2	0	-2	-3	5',
        '-1	0	-2	-3	-5	-1	-2	-3	-2	2	4	0	6',
        '-4	-4	-4	-6	-4	-5	-5	-5	-2	1	2	-5	0	9',
        '1	0	-1	-1	-3	0	-1	-1	0	-2	-3	-1	-2	-5	6',
        '1	0	1	0	0	-1	0	1	-1	-1	-3	0	-2	-3	1	3',
        '1	-1	0	0	-2	-1	0	0	-1	0	-2	0	-1	-2	0	1	3',
        '-6	2	-4	-7	-8	-5	-7	-7	-3	-5	-2	-3	-4	0	-6	-2	-5	17',
        '-3	-4	-2	-4	0	-4	-4	-5	0	-1	-1	-4	-2	7	-5	-3	-3	0	10',
        '0	-2	-2	-2	-2	-2	-2	-1	-2	4	2	-2	2	-1	-1	-1	0	-6	-2	4',
    ]
    var headerString =
        "A    R    N    D    C    Q    E    G    H    I    L    K    M    F    P    S    T    W    Y    V"
    var headers = _.str.clean(headerString).split(" ")

    var dataArray = _.map(lines, (line) => _.str.clean(line).split(" "))

    var pam250: ScoreMatrix = {}
    _.each(headers, (header) => pam250[header] = {})

    _.each(dataArray, (row, rowIndex) => {
        _.each(row, (value, colIndex) => {
            var rowHeader = headers[rowIndex]
            var colHeader = headers[colIndex]
            pam250[rowHeader][colHeader] = parseInt(value)
            pam250[colHeader][rowHeader] = parseInt(value)
        })
    })

    return pam250;
}


function alphabet(): Array<string> {
    var headerString = "A    R    N    D    C    Q    E    G    H    I    L    K    M    F    P    S    T    W    Y    V"
    var headers = _.str.clean(headerString).split(" ")
    return headers
}
// ftp://ftp.ncbi.nih.gov/blast/matrices/BLOSUM62

function blosum62(): ScoreMatrix {
    var lines = [
        ' 4',
        '-1  5',
        '-2  0  6',
        '-2 -2  1  6',
        ' 0 -3 -3  3  9',
        '-1  1  0  0 -3  5',
        '-1  0  0  2 -4  2  5',
        ' 0 -2  0 -1 -3 -2 -2  6',
        '-2  0  1 -1 -3  0  0 -2  8',
        '-1 -3 -3 -3 -1 -3 -3 -4 -3  4',
        '-1 -2 -3 -4 -1 -2 -3 -4 -3  2  4',
        '-1  2  0 -1 -3  1  1 -2 -1 -3 -2  5',
        '-1 -1 -2 -3 -1  0 -2 -3 -2  1  2 -1  5',
        '-2 -3 -3 -3 -2 -3 -3 -3 -1  0  0 -3  0  6',
        '-1 -2 -2 -1 -3 -1 -1 -2 -2 -3 -3 -1 -2 -4  7',
        ' 1 -1  1  0 -1  0  0  0 -1 -2 -2  0 -1 -2 -1  4',
        ' 0 -1  0 -1 -1 -1 -1 -2 -2 -1 -1 -1 -1 -2 -1  1  5',
        '-3 -3 -4 -4 -2 -2 -3 -2 -2 -3 -2 -3 -1  1 -4 -3 -2  11',
        '-2 -2 -2 -3 -2 -1 -2 -3  2 -1 -1 -2 -1  3 -3 -2 -2  2  7',
        ' 0 -3 -3 -3 -1 -2 -2 -3 -3  3  1 -2  1 -1 -2 -2  0 -3 -1 4',
    ]
    var headerString =
        "A    R    N    D    C    Q    E    G    H    I    L    K    M    F    P    S    T    W    Y    V"
    var headers = _.str.clean(headerString).split(" ")

    var dataArray = _.map(lines,(line) => _.str.clean(line).split(" "))

    var blosum62: ScoreMatrix = {}
    _.each(headers,(header) => blosum62[header] = {})

    _.each(dataArray,(row, rowIndex) => {
        _.each(row,(value, colIndex) => {
            var rowHeader = headers[rowIndex]
            var colHeader = headers[colIndex]
            blosum62[rowHeader][colHeader] = parseInt(value)
            blosum62[colHeader][rowHeader] = parseInt(value)
        })
    })

    return blosum62;
}


function allKLetterWords(k: number): Array<string> {
    var chars: Array<string> = alphabet()
    return _.flatten(
        _.map(chars, (l1) => {
            return _.map(chars, (l2) => {
                return _.map(chars, (l3) => {
                    return l1 + l2 + l3
                })
            })
        }))
}


// Compute word pair score given the scoringMatrix.
// Words should be of same length.
function wordPairScore(scoringMatrix: ScoreMatrix,
                       word1: string,
                       word2: string): number {
    var letterScores: Array<number> =
        _.map(_.zip(word1, word2), (pair: Array<string>) => {
            return scoringMatrix[pair[0]][pair[1]]
        })
    return _.reduce(letterScores, (acc: number, n: number) => { return acc + n }, 0)
}

function highScoringNeighbors(scoringMatrix: ScoreMatrix,
                              word: string, T: number) {
    var l = word.length
    return _.filter(allKLetterWords(l), (w) => {
        return (wordPairScore(scoringMatrix, word, w) > T)
    })
}


function findHitsInDatabase(db: string,
                            words: Array<QueryWord>): Array<Alignment> {
   return _.map(words, (word: QueryWord) => {
       return {
           queryOffset: word.queryOffset,
           dbOffset: db.indexOf(word.wordText),
           length: word.wordText.length
       }
    })
}

function scoreAlignment(a: Alignment, search: SearchParams): number {
    var queryPart = search.query.slice(a.queryOffset, a.queryOffset + a.length)
    var dbPart = search.db.slice(a.dbOffset, a.dbOffset + a.length)
    return wordPairScore(search.scoreMatrix, queryPart, dbPart)
}

function canLeftExtend(a: Alignment, search: SearchParams): Boolean {
    return a.queryOffset > 0 && a.dbOffset > 0;
}

function canRightExtend(a: Alignment, search: SearchParams): Boolean {
    return ((a.queryOffset + a.length) < search.query.length &&
            (a.dbOffset + a.length) < search.db.length)
}

function extendAlignmentLeft(a: Alignment, search: SearchParams): Alignment {
    if (canLeftExtend(a, search)) {
        return {
            queryOffset: a.queryOffset - 1,
            dbOffset: a.dbOffset - 1,
            length: a.length + 1
        }
    } else
        return a
}

function extendAlignmentRight(a: Alignment, search: SearchParams): Alignment {
    if (canRightExtend(a, search)) {
        return {
            queryOffset: a.queryOffset,
            dbOffset: a.dbOffset,
            length: a.length + 1
        }
    } else
        return a
}

function findMaxAlignmentLeft(a: Alignment,
                              search: SearchParams): Alignment
{
    return findMaxAlignment(a, extendAlignmentRight, canRightExtend, search)
}

function findMaxAlignmentRight(a: Alignment,
                               search: SearchParams): Alignment
{
    return findMaxAlignment(a, extendAlignmentLeft, canLeftExtend, search)
}

function findMaxAlignment(a: Alignment,
                          extendAlignment: ExtendAlignmentFn,
                          canExtend: CanExtendAlignmentFn,
                          search: SearchParams): Alignment
{
    var currentAlignment = a

    // hold the peak score and alignment so we can go back
    var peakScore = currentScore
    var peakAlignment = a


    var currentScore = scoreAlignment(a, search)
    var dropOff = currentScore - peakScore

    while (dropOff < search.allowedDropOff && canExtend(a, search)) {
        currentAlignment = extendAlignment(a, search)

        currentScore =
            scoreAlignment(currentAlignment, search)

        if (currentScore > peakScore) {
            // found a new peak
            peakScore = currentScore
            peakAlignment = currentAlignment
        } else {
            // found a new cliff
            dropOff = peakScore - currentScore
        }
    }

    if (dropOff < search.allowedDropOff) {
        return peakAlignment
    } else {
        return currentAlignment
    }
}