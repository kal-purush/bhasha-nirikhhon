import numpy as np

def find_replace_string(s, indices, sources, targets):
    out = ""
    illegal = set()

    indices = np.array(indices)
    sources = np.array(sources)
    targets = np.array(targets)

    guide = indices.argsort()
    indices = indices[guide].tolist()
    sources = sources[guide].tolist()
    targets = targets[guide].tolist()

    curr_interval = [indices[0], indices[0]+len(sources[0])]

    if s[curr_interval[0]:curr_interval[1]]!=sources[0]:
        illegal.add(indices[0])

    for i in range(1, len(indices)):
        start = indices[i]
        end = indices[i] + len(sources[i])
        if s[start:end]!=sources[i]:
            illegal.add(indices[i])

        if start<curr_interval[1]: # We have an intersection
            illegal.add(indices[i-1])
            illegal.add(indices[i])

            curr_interval = [min(start, curr_interval[0]), max(end, curr_interval[1])]
        else:
            curr_interval = [start, end]

    i = 0
    k = 0
    while k<len(indices):
        if i==indices[k]:
            if i in illegal:
                out += s[i]
                i += 1 
                k += 1
            else:
                out += targets[k]
                i += len(sources[k])
                k += 1
        elif i>indices[k]:
            k += 1
        else:
            out += s[i]
            i += 1

    while i<len(s):
        out += s[i]
        i += 1

    return out

s = "a"
indices = [0]
sources = ["a"]
targets = ["bbbb"]
find_replace_string(s, indices, sources, targets)

s = "abcd"
indices = [0,2]
sources = ["a", "cd"]
targets = ["eee", "ffff"]
find_replace_string(s, indices, sources, targets)

s = "abcd"
indices = [0, 2]
sources = ["ab","ec"]
targets = ["eee","ffff"]
find_replace_string(s, indices, sources, targets)

s = "abcdef"
indices = [0, 2, 3, 5]
sources = ["ac", "cde", "de", "f"]
targets = ["aaaa", "gggg", "ffff", "yesitworks"]
find_replace_string(s, indices, sources, targets)

s = "jjievdtjfb"
indices = [4,6,1]
sources = ["md","tjgb","jf"]
targets = ["foe","oov","e"]
find_replace_string(s, indices, sources, targets)