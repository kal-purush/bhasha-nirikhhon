def p_reader():
    eq_input = input("Input equation : ")

    brac_ctr = 0
    stor_lst = []
    mini_eq_lst = []

    for c in eq_input:

        if c == '(':
            brac_ctr += 1
        elif c == ')':
            brac_ctr -= 1
            temp = stor_lst.copy()
            print(stor_lst, temp)
            input("Inp")
            if len(mini_eq_lst) > 0:
                mini_eq_lst.append(list(set(stor_lst) - set(temp)))
            mini_eq_lst.append(temp)
        else:
            stor_lst.append(c)

    print(mini_eq_lst)


p_reader()