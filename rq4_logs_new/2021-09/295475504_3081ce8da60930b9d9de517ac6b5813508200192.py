def zerofy_matrix_inplace(matrix):
    """place complexity is O(1) and time complexity is O(NM)
    """
    M = len(matrix) # number of rows
    N = len(matrix[0])  # number of columns
    # two boolean variables for the common element at [0][0]
    row = False
    column = False

    for i in range(M):
        for j in range(N):
            if matrix[i][j] == 0:
                if i == 0 and j == 0:
                    row = True
                    column = True
                elif i == 0 and j != 0:
                    row = True
                    matrix[0][j] = 0
                elif j == 0 and i != 0:
                    column = True
                    matrix[i][0] = 0
                else:
                    matrix[0][j] = 0
                    matrix[i][0] = 0                
        
    for i in range(1, M):
        if matrix[i][0] == 0:
            matrix[i] = [0] * N

    for j in range(1, N):
        if matrix[0][j] == 0:
            matrix = [[elem if k != j else 0 for k, elem in enumerate(row)] for row in matrix]
    
    if row == True:
        matrix[0] = [0] * N
    
    if column == True:
        matrix = [[0 if j == 0 else elem for j, elem in enumerate(layer)] for layer in matrix]

    return matrix

def rotate_matrix_inplace(matrix):
    """
    rotates a matrix 90 degrees clockwise without the need for extra space, O(1)
    """
    # loop for number of squares/cycles we have
    for i in range(len(matrix) // 2):
        span = len(matrix) - 1
        # loop for number of elements/sqaure that needs to be rotated
        for j in range(i, span - i):
            # 4 elements are swapped in each iteration
            tmp = matrix[i][j]
            matrix[i][j] = matrix[span - j][i]
            matrix[span - j][i] = matrix[span - i][span - j]
            matrix[span - i][span - j] = matrix[j][span -i]
            matrix[j][span - i] = tmp
    
    return matrix

def string_compression(string):
    """
    run-time is O(N)
    space is O(N)
    """
    if len(string) == 0:
        return ""
    index1 = 0
    letter_count = 1
    lstring = []
    for index2 in range(1, len(string)):
        if string[index1] != string[index2]:
            lstring.append(string[index1] + str(letter_count))
            index1 = index2
            letter_count = 1
        else:
            letter_count += 1

    lstring.append(string[index1] + str(letter_count))
    compressed_string = "".join(lstring)
    if len(compressed_string) >= len(string):
        return string

    return compressed_string

def is_one_away(string1, string2):
    """
    """
    if abs(len(string1) - len(string2)) > 1:
        return False

    index1, index2 = 0, 0
    count_differences = 0
    while index1 <= len(string1) - 1 and index2 <= len(string2) - 1:
        if string1[index1] == string2[index2]:
            index1 += 1
            index2 += 1
        elif len(string1) == len(string2):
            count_differences += 1
            index1 += 1
            index2 += 1
        elif len(string1) > len(string2):
            count_differences += 1
            index1 += 1
        elif len(string2) > len(string1):
            count_differences += 1
            index2 += 1

        if count_differences > 1:
            return False
    
    return True

def is_palindrome_permetation(phrase):
    """
    runtime is O(N)
    space is O(N)
    """
    char_dict = {}
    for char in phrase.casefold():
        if char == " ":
            continue
        if char in char_dict:
            char_dict[char] += 1
        else:
            char_dict[char] = 1

    # number of chars with odd occurences
    num_odd_chars = 0
    for key in char_dict.keys():
        if char_dict[key] % 2 == 0:
            continue
        else:
            num_odd_chars += 1
    
    if num_odd_chars <= 1:
        return True
    
    return False

def URLify(str, length):
    """
    The length is the actual length of the string: length = len(str) + 2 * #spaces. The run-time complexity is O(N), where N is the length of str. The space complexity is O(1).
    """
    print(f'The old string is: {str}')
    lstr = list(str)
    index1 = len(lstr) - 1
    # catch the position of the last letter in the string
    index2 = length - 1

    while index1 != index2:
        if lstr[index2] != " ":
            lstr[index1] = lstr[index2]
        else:
            lstr[index1] = "0"
            index1 -= 1
            lstr[index1] = "2"
            index1 -= 1
            lstr[index1] = "%"

        index1 -= 1
        index2 -= 1

    return "".join(lstr)

if __name__ == "__main__":
    """
    s = "Hello Mr John Smith      "
    updated_s = URLify(s, 19)
    print(f'The new string is: {updated_s}')
    """

    """
    phrase = "azAZ"
    print(f"Is the phrase {phrase} a palindrome permetation? {is_palindrome_permetation(phrase)}")
    """

    """
    s1, s2 = "pale", "pales'"
    print(f"Are {s1} and {s2} one change away? {is_one_away(s1, s2)}")
    """

    """
    s = "aabcccccaaa" #"a2b1c5a3"
    print(f"The compressed version of {s} is {string_compression(s)}")
    """

    """
    # m = [[1, 2, 3], [4, 5, 6], [7, 8, 9]] #[[7, 4, 1], [8, 5, 2], [9, 6, 3]]
    m =  [[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11, 12], [13, 14, 15, 16, 17, 18], [19, 20, 21, 22, 23, 24], [25, 26, 27, 28, 29, 30], [31, 32, 33, 34, 35, 36]]
    print(f"The rotated matrix is {rotate_matrix_inplace(m)}")
    """
    m = [
                [1, 2, 3, 4, 0],
                [6, 0, 8, 9, 10],
                [11, 12, 13, 14, 15],
                [16, 0, 18, 19, 20],
                [21, 22, 23, 24, 25],
            ]
    m1 = [[0,1,2,0],[3,4,5,2],[1,3,1,5]]
    print(f"The new matrix is {zerofy_matrix_inplace(m1)}")
    
    