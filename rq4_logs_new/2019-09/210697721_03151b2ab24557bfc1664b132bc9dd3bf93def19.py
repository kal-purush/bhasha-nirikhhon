#!/usr/bin/env python

def _compute_prefix(pattern):
    pattern_length = len(pattern)
    prefix = {
        0 : 0
    }
    
    for index in range(1,pattern_length):
        k = prefix[index-1]
        while k>0 and pattern[index] != pattern[k]:
            k = prefix[k-1]
        if pattern[index] == pattern[k]:
            k = k+1
        prefix[index] = k

    return prefix


def _kmp_matcher(string,pattern):
    prefix = _compute_prefix(pattern)
    string_length = len(string)
    pattern_length = len(pattern)

    found_at_indices = []
    index = 0
    q = 0

    dna = {
        'a' : 't',
        't' : 'a',
        'g' : 'c',
        'c' : 'g'
    }
    
    while index < string_length:
        if dna[pattern[q]] == string[index]:
            q = q+1
            index = index + 1
        if q == pattern_length:
            found_at_indices.append(index-pattern_length)
            q = prefix[q-1]
        elif index < string_length and dna[pattern[q]] != string[index]:
            if q != 0:
                q = prefix[q-1] 
            else:
                index = index + 1

    return found_at_indices


def _get_substring_indices(pattern):
    end_index = len(pattern) - 1
    return { pattern[j:j+i+1] : {'start' : j, 'end' : j+i} for i in range(end_index,-1,-1) for j in range(0,end_index+1) if j+i <= end_index and i+2>6 }
               

if __name__ == "__main__":
    file1 = open("user1.txt","r")
    file2 = open("user2.txt","r")
    file3 = open('output.txt','w')
    dna = {
        'a' : 't',
        't' : 'a',
        'g' : 'c',
        'c' : 'g'
    }

    program_output = ''
    
    string = file1.readline()
    print()
    string = string.lower()
    if set(string).union(set('atgc')) != set('atgc'):
        #print()
        program_output += 'Invalid RNA sequence !!\n'
        quit()
        
    pattern = file2.readline()
    print()
    pattern = pattern.lower()
    if set(pattern).union(set('atgc')) != set('atgc'):
        #print('Invalid Messenger RNA sequence !!')
        program_output += 'Invalid Messenger RNA sequence !!\n'
        quit()
    if len(pattern) < 6:
        #print('Messenger RNA sequence should be a minimum of 6 nucleotides long !!')
        program_output += 'Messenger RNA sequence should be a minimum of 6 nucleotides long !!\n'
        quit()

    pattern_map = _get_substring_indices(pattern)

    #print()
    #print()
    #print()
    program_output += 3*'\n'
    match_map = {}
    for pat in pattern_map.keys():
        found_at_indices = _kmp_matcher(string,pat)
        if found_at_indices:
            for i in found_at_indices:
                if i in match_map:
                    if len(pat)>len(match_map[i]):
                        match_map[i] = pat
                else:
                    match_map[i] = pat

            match_indices = []
            for i in match_map.keys():
                s = i-1
                p = pattern_map[ match_map[i] ]['start'] - 1
                while s>=0 and p>=0:
                    if string[s] == dna[ pattern[p] ]:
                        match_indices.insert(1,s)
                    s = s-1
                    p = p-1

                s = i
                p = pattern_map[ match_map[i] ]['start']
                while s<len(string) and p<len(pattern):
                    if string[s] == dna[ pattern[p] ]:
                        match_indices.append(s)
                    s = s+1
                    p = p+1


                
            #print(len(pattern)*' ',end="")
            program_output += len(pattern)*' '
            for c in string:
                #print(c.upper(),end="")
                program_output += c.upper()
            #print()
            program_output += '\n'
            
            #print(len(pattern)*' ',end="")
            program_output += len(pattern)*' '
            for i in range(len(string)):
                if i in match_indices:
                    #print('|',end="")
                    program_output += '|'
            #print()
            program_output += '\n'

            #print()
            program_output += '\n'
            #print((len(pattern)+match_indices[0]-pattern_map[ pat ]['start']-1)*' ',pattern.upper())
            program_output += (len(pattern)+match_indices[0]-pattern_map[ pat ]['start']-1)*' ' + pattern.upper()
            #print()
            #print()
            program_output += 2*'\n'

    if not match_map.keys():
        #print('No matches of the Messenger RNA found in the RNA sequence')
        program_output += 'No matches of the Messenger RNA found in the RNA sequence'

    print(program_output)
    file3.write(program_output)
    file1.close()
    file2.close()
    file3.flush()
    file3.close()