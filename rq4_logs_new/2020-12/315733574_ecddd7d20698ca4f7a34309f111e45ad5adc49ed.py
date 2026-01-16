def main(first_str, second_str):
    if type(first_str) != str or type(second_str) != str:
        return 0
    elif len(first_str) == len(second_str):
        return 1
    elif len(first_str) > len(second_str):
        return 2
    elif second_str == 'learn':
        return 3


if __name__ == "__main__":
    print(main({'name':'learn'}, 123))            #0
    print(main('Learn', 'learn'))                 #1
    print(main('python', 'Learn'))                #2
    print(main('do', 'learn'))                    #3
    print(main('python', 'learn'))                #2
    print(main(['learn','python','ru'], 'learn')) #0
    print(main('Learn', 'python'))                #None