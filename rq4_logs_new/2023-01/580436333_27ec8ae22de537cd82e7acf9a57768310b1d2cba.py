
input = "abc"
output = []
def subset(input,output):
    
    # Base Condition
    if(len(input) == 0):
        output.insert(0," ")
        return 1
    
    smallString = input[1:]
    # recursive call
    size = subset(smallString,output)
    for i in range(0,size):
        # output[i+size] = input[0] + output[i]
        output.insert(i+size,input[0] + output[i])

    return 2*size


def getKeypadText(num):
    switcher = {
        2:"abc",
        3:"def",
        4:"ghi",
        5:"jkl",
        6:"mno",
        7:"pqrs",
        8:"tuv",
        9:"wxyz",
    }

    return switcher.get(num)

output2 = []
# def keypad(num,output):
    
#     # Base Condition
#     if(num==0):
#         output.insert(0," ")
#         return 1

#     string = getKeypadText(num%10)
#     print(len(string))
#     smallOutputSize = keypad(num/10,output)
#     ans_size = smallOutputSize * len(string)
#     print(len(string))
#     k=0
#     temp = []
#     for i in range(0,smallOutputSize):
#         for j in range (0,len(string)):
#             # temp[k] = output[i] + string[j]
#             temp.insert(k,output[i] + string[j])
#             k +=1

#     for i in range (0,ans_size):
#         output[i] = temp[i]
#     return ans_size

def printSubsets(size):
    for i in range (0,size):
        print(output[i])

if __name__ == "__main__":
    # size = subset(input,output)
    size = keypad(23,output2)
    printSubsets(size)


