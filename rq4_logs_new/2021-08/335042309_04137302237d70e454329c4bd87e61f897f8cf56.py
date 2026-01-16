def squeeze(s): # Turn heeellooo -> 1h3e2l3o

    out = ""
    counts = []
    curr_count = 1
    curr_char = s[0]
    chars = set(s)

    for i in range(1,len(s)):
        if s[i]!=curr_char:
            out += curr_char
            counts.append(curr_count)
            curr_char = s[i]
            curr_count = 1
        else:
            curr_count += 1 
    
    if s[-1]==curr_char:
        out += curr_char
        counts.append(curr_count)
    else:
        out += curr_char
        counts.append[1]

    return out, counts 

squeeze("heeellooo")
squeeze("h")
squeeze("hhhhhhheeeehhhh")

def expressive_words(s, words):

    sq_s, cs = squeeze(s)
    out = 0 

    for w in words:

        if s==w:
            out += 1
            continue 

        sq_w, cw = squeeze(w)
        f = 0

        if len(sq_s)!=len(sq_w): # Different characters present
            continue 
            
        for i in range(len(sq_w)):
            if sq_w[i]!=sq_s[i]: # Characters are different
                f = 1
                break 
            elif cw[i]>cs[i]: # Word has more characters in a group than
                f = 1
                break
            elif cs[i]==2 and cw[i]<2:
                f = 1
                break
        
        if f==1:
            continue 
        out += 1
    
    return out 

s = "heeellooo"
words = ["hello", "hi", "helo"]
expressive_words(s, words)

s = "zzzzzyyyyy"
words = ["zzyy","zy","zyy"]
expressive_words(s, words)

s = "aaaa"
words = ["aaaaaa"]
expressive_words(s, words)

s = "sasss"
words = ["sa"]
expressive_words(s, words)