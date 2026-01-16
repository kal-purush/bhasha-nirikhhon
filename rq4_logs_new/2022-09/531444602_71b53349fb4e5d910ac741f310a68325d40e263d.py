# Understanding dictionary, printing content using keys method
def dictionary_post():
    post = {"userid": 'junesuchi', "location": (
        12454.12, 414512), "language": 'English', "ID": 150}
    print("Below content is the results of using keys method \n")
    for n in post.keys():
        value = post[n]
        print(n, "=", post[n])


# Dictionary, printing key and value using items method
def dictionarypostwithitems():
    post = {"userid": 'junesuchi', "location": (
        12454.12, 414512), "language": 'English', "ID": 150}
    print("Below content is the results of using items method \n")
    for n, value in post.items():
        print(n, "=", value)


# dictionary ,pop items out of dictionary using pop
def popkeysfromdict1():
    post = {"userid": 'junesuchi', "location": (
        12454.12, 414512), "language": 'English', "ID": 150}
    print("Below content is the results of using pop method \n")
    k = post.pop("userid")
    print(post, k)


dictionary_post()
dictionarypostwithitems()
popkeysfromdict1()