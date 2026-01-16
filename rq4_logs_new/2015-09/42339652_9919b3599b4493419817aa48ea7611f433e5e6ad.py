with open("1.1.2_value.hex") as f:
    file_content = f.read().strip()
binary_content = file_content.decode('hex')
integer_parsed = int(file_content,16)
f1 = open('sol_1.1.2_decimal.txt', 'r+');
f1.write(str(integer_parsed))
f1.closed

binary=bin(integer_parsed)[2:]
f2=open('sol_1.1.2_binary.txt', 'r+');
f2.write(str(binary))
f2.closed