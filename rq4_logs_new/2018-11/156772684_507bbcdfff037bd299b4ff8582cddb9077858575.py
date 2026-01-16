# 4-bit boolean algebra converter script
# Outputs proper Vivado code equivalent to simple-format boolean algebra input
# Notes: spaces are converted to boolean AND "&", plus signs are converted to boolean OR "|"
# Inverted inputs are input as a' where a is the non-inverted input

inputs = [
    "a' b c' d'+a' b' c' d+a b c' d+a b' c d",
    # "a c d+b c d'+a' b c' d+a b d'",
    # "a' b' c d'+a b d'+a b c",
    # "a' b c' d'+b' c' d+b c d+a b' c d'",
    # "a' d+a' b c'+b' c' d",
    # "a' b' d+a' b' c+a' c d+a b c' d",
    # "a' b' c'+a' b c d+a b c' d'"
]

if __name__ == '__main__':
    for _input in inputs:
        final = "assign x = "
        replaced = _input.replace("a'", "(~A)")\
            .replace("b'", "(~B)")\
            .replace("c'", "(~C)")\
            .replace("d'", "(~D)")\
            .replace("+", "|")\
            .replace(" ", "&")\
            .replace("a", "A")\
            .replace("b", "B")\
            .replace("c", "C")\
            .replace("d", "D")
        split = replaced.split("|")
        for i in range(len(split) - 1):
            final += "(" + split[i] + ") | "
        final += "(" + split[len(split) - 1] + ");"
        print(final)