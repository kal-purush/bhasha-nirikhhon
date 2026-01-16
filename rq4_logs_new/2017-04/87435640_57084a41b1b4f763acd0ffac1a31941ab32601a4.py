


def func(str_name,val): # set variable with str_name with value val

    if str_name in globals() : # already a global variable
        exec (str_name + "=" + val);

    if str_name not in globals(): # not a global variable
        exec ("global "+str_name+"; "+ str_name+"="+val);




func ("t","5");
print t;
func ("t","7");
print t;
func("u","8")
print u;