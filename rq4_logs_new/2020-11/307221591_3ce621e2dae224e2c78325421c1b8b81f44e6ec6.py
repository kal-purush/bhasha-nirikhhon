import matplotlib.pyplot as plt

import mnist

(train_data, train_label), (test_data, test_label) = mnist.load_data()

def show_data_gui(digit, datatype):
    if (datatype == "train"):
        data = train_data[digit]
        label = str(train_label[digit])
    elif (datatype == "test"):
        data = test_data[digit]
        label = str(test_label[digit])
    else:
        sys.stdout.write("Error : Please input \"train\" or \"test\" as datatype.\n")
        sys.exit(3)
    plt.title(str(datatype) + str(digit) + " : " + label)
    plt.imshow(data, cmap=plt.cm.binary)
    plt.show()

def show_data_cli(digit, datatype):
    for i in range(28):
        for j in range(28):
            if (datatype == "train"):
                print('{:>3}'.format(train_data[digit][i][j]), end=" ")
                label = str(train_label[digit])
            elif (datatype == "test"):
                print('{:>3}'.format(test_data[digit][i][j]), end=" ")
                label = str(test_label[digit])
            else:
                sys.stdout.write("Error : Please input \"train\" or \"test\" as datatype.\n")
                sys.exit(3)
        print("\n")
    print("Label : " + label)

def show_basic_info():
    print("Data ndim：" + str(train_data.ndim))
    print("Data shape：" + str(train_data.shape))
    print("Data data type：" + str(train_data.dtype))
    print("Label ndim：" + str(train_label.ndim))
    print("Label shape：" + str(train_label.shape))
    print("Label data type：" + str(train_label.dtype))