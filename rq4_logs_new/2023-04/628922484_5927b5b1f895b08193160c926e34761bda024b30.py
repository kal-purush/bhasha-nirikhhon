en_list = ["english.0", "english.1", "english.2", "english.3"]
out_dict = list()


def check_suffix(word: str, suffix) -> str:
    if suffix == "V":
        # flag V:
        #     E		>	-E,IVE		# As in create > creative
        #     [^E]	>	IVE		# As in prevent > preventive
        if word[-1] == "e":
            return word[:-1] + "ive"
        else:
            return word + "ive"
    if suffix == "N":
        # flag *N:
        #     E		>	-E,ION		# As in create > creation
        #     Y		>	-Y,ICATION	# As in multiply > multiplication
        #     [^EY]	>	EN		# As in fall > fallen
        if word[-1] == "e":
            return word[:-1] + "ion"
        elif word[-1] == "y":
            return word[:-1] + "ication"
        else:
            return word + "en"
    if suffix == "X":
        # flag *X:
        #     E		>	-E,IONS		# As in create > creations
        #     Y		>	-Y,ICATIONS	# As in multiply > multiplications
        #     [^EY]	>	ENS		# As in weak > weakens
        if word[-1] == "e":
            return word[:-1] + "ions"
        elif word[-1] == "y":
            return word[:-1] + "ications"
        else:
            return word + "ens"
    if suffix == "H":
        # flag H:
        #     Y		>	-Y,IETH		# As in twenty > twentieth
        #     [^Y]	>	TH		# As in hundred > hundredth
        if word[-1] == "y":
            return word[:-1] + "ily"
        else:
            return word + "ing"
    if suffix == "G":
        # flag *Y:
        #     Y		>	-Y,ILY		# As in messy > messily
        #     [^Y]	>	LY		# As in quick > quickly
        if word[-1] == "y":
            return word[:-1] + "ily"
        else:
            return word + "ly"
    if suffix == "G":
        # flag *G:
        #     E		>	-E,ING		# As in file > filing
        #     [^E]	>	ING		# As in cross > crossing
        if word[-1] == "e":
            return word[:-1] + "ing"
        else:
            return word + "ing"
    if suffix == "J":
        # flag *J:
        #     E		>	-E,INGS		# As in file > filings
        #     [^E]	>	INGS		# As in cross > crossings
        if word[-1] == "e":
            return word[:-1] + "ings"
        else:
            return word + "ings"
    if suffix == "D":
        # flag *D:
        #     E		>	D		# As in create > created
        #     [^AEIOU]Y	>	-Y,IED		# As in imply > implied
        #     [^EY]	>	ED		# As in cross > crossed
        #     [AEIOU]Y	>	ED		# As in convey > conveyed
        if word[-1] == "e":
            return word + "d"
        if word[-1] == "y":
            if word[-2] not in "aeiou":
                return word[:-1] + "ied"
            else:
                return word + "ed"
        else:
            return word + "ed"
    if suffix == "T":
        # flag T:
        #     E		>	ST		# As in late > latest
        #     [^AEIOU]Y	>	-Y,IEST		# As in dirty > dirtiest
        #     [AEIOU]Y	>	EST		# As in gray > grayest
        #     [^EY]	>	EST		# As in small > smallest
        if word[-1] == "e":
            return word + "st"
        if word[-1] == "y":
            if word[-2] not in "aeiou":
                return word[:-1] + "iest"
            else:
                return word + "est"
        else:
            return word + "est"
    if suffix == "R":
        # flag *R:
        #     E		>	R		# As in skate > skater
        #     [^AEIOU]Y	>	-Y,IER		# As in multiply > multiplier
        #     [AEIOU]Y	>	ER		# As in convey > conveyer
        #     [^EY]	>	ER		# As in build > builder
        if word[-1] == "e":
            return word + "r"
        if word[-1] == "y":
            if word[-2] not in "aeiou":
                return word[:-1] + "ier"
            else:
                return word + "er"
        else:
            return word + "er"
    if suffix == "Z":
        # flag *Z:
        #     E		>	RS		# As in skate > skaters
        #     [^AEIOU]Y	>	-Y,IERS		# As in multiply > multipliers
        #     [AEIOU]Y	>	ERS		# As in convey > conveyers
        #     [^EY]	>	ERS		# As in build > builders
        if word[-1] == "e":
            return word + "rs"
        if word[-1] == "y":
            if word[-2] not in "aeiou":
                return word[:-1] + "iers"
            else:
                return word + "ers"
        else:
            return word + "ers"
    if suffix == "S":
        # flag *S:
        #     [^AEIOU]Y	>	-Y,IES		# As in imply > implies
        #     [AEIOU]Y	>	S		# As in convey > conveys
        #     [CS]H	>	ES		# As in lash > lashes
        #     [^CS]H	>	S		# As in cough > coughs
        #     [SXZ]	>	ES		# As in fix > fixes
        #     [^SXZHY]	>	S		# As in bat > bats
        if word[-1] == "y":
            if word[-2] not in "aeiou":
                return word[:-1] + "ies"
            else:
                return word + "s"
        if word[-1] == "h":
            if word[-2] not in "cs":
                return word[:-1] + "s"
            else:
                return word + "es"
        else:
            if word[-1] in "sxz":
                return word + "ers"
            else:
                return word + "s"
    if suffix == "P":
        # flag *P:
        #     [^AEIOU]Y	>	-Y,INESS	# As in cloudy > cloudiness
        #     [AEIOU]Y	>	NESS		# As in gray > grayness
        #     [^Y]	>	NESS		# As in late > lateness
        if word[-1] == "y":
            if word[-2] not in "aeiou":
                return word[:-1] + "iness"
            else:
                return word + "ness"
        else:
            return word + "ness"
    if suffix == "M":
        # flag *M:
        #     .		>	'S		# As in dog > dog's
        return word + "'s"
    return word


def parse(word: str):
    spl = word.split("/")
    if len(spl) == 1:
        yield spl[0]
    else:
        for each in spl[1]:
            res = check_suffix(spl[0], each)
            if res is not None:
                yield res + "\n"


for each in en_list:
    with open(each, "r") as f:
        for line in f.readlines():
            for each in parse(line):
                out_dict.append(each.lower())


file = open("combined.txt", "w")
for each in out_dict:
    file.write(each)
file.close()