import string

with open("moby_01.txt") as infile, open("moby_01_clean.txt", "w") as outfile:
    for line in infile:
        # 將所有字元轉成小寫
        cleaned_line = line.lower()
        # 刪除所有標點符號
        punc = string.punctuation
        for c in punc:
            cleaned_line=cleaned_line.replace(c," ")
        # 依照單字切割
        #print(cleaned_line)
        words = cleaned_line.split()
        #print(words)
        cleaned_words = "\n".join(words)
        #print(cleaned_words)
         # 每行放一個單字
        outfile.write(cleaned_words)