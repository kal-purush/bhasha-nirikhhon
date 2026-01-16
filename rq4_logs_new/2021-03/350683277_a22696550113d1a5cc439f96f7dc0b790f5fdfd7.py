def main():
    ctrl = True
    while(ctrl):
        menu()
        option = input("please choose:")
        option_str = re.sub("\D", "", option)
        if option_str in ['0', '1']:
            option_int = int(option_str)
            if option_int == 0:
                print('您已退出系统！')
                ctrl = False
            elif option_int == 1:
                insert()
            elif option_int == 2:
                search()
            elif option_int == 3:
                delete()
            elif option_int == 4:
                modify()
            elif option_int == 5:
                sort()
            elif option_int == 6:
                total()
            elif option_int == 7:
                show()