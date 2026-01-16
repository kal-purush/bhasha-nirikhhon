import check50
import check50.c

@check50.check()
def exists():
    """scrabble.c exists"""
    check50.exists("scrabble.c")

@check50.check(exists)
def compiles():
    """scrabble.c compiles"""
    check50.c.compile("scrabble.c", lcs50=True)

@check50.check(compiles)
def rejects_0_args():
    """rejects 0 arguments entered at the command line with exit code 1"""
    check50.run("./scrabble").exit(1)

@check50.check(compiles)
def rejects_2_args():
    """rejects 2 arguments entered at the command line with exit code 1"""
    check50.run("./scrabble").exit(1)

@check50.check(compiles)
def computes_a_as_1():
    """correctly calculates the score for "a" as "0""""
    check50.run("./scrabble a").stdout("1").exit(0)

@check50.check(compiles)
def computes_A_as_1():
    """correctly calculates the score for "A" as "0""""
    check50.run("./scrabble A").stdout("1").exit(0)

@check50.check(compiles)
def computes_king_as_9():
    """correctly calculates the score for "king" as "9""""
    check50.run("./scrabble king").stdout("9").exit(0)

@check50.check(compiles)
def computes_King_as_9():
    """correctly calculates the score for "King" as "9""""
    check50.run("./scrabble King").stdout("9").exit(0)

@check50.check(compiles)
def computes_KING_as_9():
    """correctly calculates the score for "KING" as "9""""
    check50.run("./scrabble KING").stdout("9").exit(0)

@check50.check(compiles)
def rejects_no_letters():
    """rejects "!2#4" entered at the command line with exit code 2"""
    check50.run("./scrabble !2#4").exit(2)

@check50.check(compiles)
def rejects_non_letters_except_underscore():
    """rejects "k1ng" entered at the command line with exit code 2"""
    check50.run("./scrabble k1ng").exit(2)

@check50.check(compiles)
def computes_king_with_underscore_as_8():
    """correctly calculates the score for "k_ng" as "8""""
    check50.run("./scrabble k_ng").stdout("8").exit(0)