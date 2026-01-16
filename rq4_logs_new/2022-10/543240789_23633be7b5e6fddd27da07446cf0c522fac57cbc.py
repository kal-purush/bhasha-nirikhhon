class UserMainCode(object):
    @classmethod

    def checkBraces(cls,input1):
        ls = list(input1)
        fr = 0
        fl = 0
        for i in ls:
            if '{' == i or '('==i or '['==i:
                fr +=1
            elif '}' == i or ')' == i or ']' == i:
                fl += 1
        if fl == fr:
            return 'correct'
        else:
            return 'error'

    # txt = 'class Main {public static void main(String[] args) {System.out.println("Enter two numbers");int first = 10);int second = 20;System.out.println(first + " " + second;// add two numbersint sum = first + second;System.out.println("The sum is: " + sum);}}'

    # print(brackets(txt))