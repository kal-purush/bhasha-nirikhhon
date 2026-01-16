info = b"hello"
infos = str(info, encoding="utf-8")
file = "file:///d:/www.python.org/sdfsd"
print(info)
print(infos)
print(type(info), type(infos))

print("{:10.10s} {:30s}".format(infos, infos))
print("{:10.10s} {:30s}".format("\t" + infos, infos))

print("{:-^15.5s}{:20}".format((" " * 5) + "0123456789" * 2, "abcd efgh ijkl mnop qrst uvwx yz"))

tabs = "\t\t"
spaces = (len(tabs) * 4 * " ")
print(len(spaces))