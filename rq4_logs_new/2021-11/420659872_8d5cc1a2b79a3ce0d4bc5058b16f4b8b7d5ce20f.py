import numpy
import numpy as np


# done
def ip(input_data):
    """
    Tính toán hàm hoán vị IP
    :param input_data: numpy array shape (8, 8)
    :return: output_data: numpy array shape (8, 8)
    """

    def matrix_index_ip():
        x = np.array([[i for i in range(1, 65)]]).reshape((8, 8))
        x_0 = np.zeros(x.shape, dtype=int)
        x_0[0] = x[:, 1]
        x_0[1] = x[:, 3]
        x_0[2] = x[:, 5]
        x_0[3] = x[:, 7]
        x_0[4] = x[:, 0]
        x_0[5] = x[:, 2]
        x_0[6] = x[:, 4]
        x_0[7] = x[:, 6]
        x_0 = x_0[::1, ::-1]
        return x_0

    return transpose(input_data, matrix_index_ip())


def split_lo_ro(input_data):
    """
    Chia đầu vào thành 2 phần L0 và R0
    :param input_data: numpy array shape (8, 8)
    :return: 2 numpy array shape (, 32)
    """
    input_data = input_data.flatten()
    return input_data[:32], input_data[32:]


def fiestel(a, j):
    """
    Tính toán hàm f
    :param a: 32 bit
    :param j: 48 bit
    :return:
    """
    print()
    e_a = numpy.array([a[31]] + a[0:5].tolist() + a[3:9].tolist() + a[7:13].tolist() + a[11:17].tolist() +
                      a[15:21].tolist() + a[19:25].tolist() + a[23:29].tolist() + a[27:].tolist() + [a[0]])

    after_xor = np.logical_xor(e_a.reshape((8, 6)), j).astype(int)

    s_boxes = get_s_boxes()

    output_f = []

    for stt, i in enumerate(after_xor):
        row = int('0b' + str(i[0]) + str(i[5]), 2)
        col = int('0b' + str(i[1]) + str(i[2]) + str(i[3]) + str(i[4]), 2)
        output_f += "{0:04b}".format(int(hex(s_boxes[stt][row, col]), 16)).strip(" ")
    output_f = np.array(output_f, dtype=int).reshape((8, 4))
    return transpose(output_f, p_index()).reshape(32, )


def transpose(data, index):
    """
    Hàm hoán vị
    :param data: ma trận cần hoán vị
    :param index: ma trận index
    :return:
    """
    output = np.zeros(data.shape, dtype=int)
    for row in range(data.shape[0]):
        for col in range(data.shape[1]):
            output[row, col] = data.item(index.item((row, col)) - 1)
    return output


def list_keys(input_k):
    """
    Tính toán và trả về mảng chứa 16 khoá k (1->16)
    :param input_k: ma trận 8x8
    :return: list()
    """
    def matrix_index_pc_1():
        x = np.array([[i for i in range(1, 65)]]).reshape((8, 8))
        x_0 = np.zeros(x.shape, dtype=int)
        x_0[0] = x[:, 0]
        x_0[2] = x[:, 2]
        x_0[6] = x[:, 4]
        x_0[4] = x[:, 6]
        x_0[1] = x[:, 1]
        x_0[3] = x[:, 3]
        x_0[5] = x[:, 5]
        x_0[7] = x[:, 7]
        x_0 = x_0[::1, ::-1]
        return np.append(np.delete(np.delete(x_0, 7, 0).reshape((8, 7)).flatten(), [28, 29, 30, 31]),
                         [28, 20, 12, 4]).reshape((8, 7))

    def matrix_index_pc_2():
        return np.array([[14, 17, 11, 24, 1, 5, 3, 28, 15, 6, 21, 10, 23, 19, 12, 4, 26, 8, 16, 7, 27, 20, 13, 2,
                          41, 52, 31, 37, 47, 55, 30, 40, 51, 45, 33, 48, 44, 49, 39, 56, 34, 53, 46, 42, 50, 36,
                          29, 32]], dtype=int).reshape((8, 6))

    def ls_vector(vector, index):
        if index in [1, 2, 9, 16]:
            value_temp = vector[0]
            return np.append(np.delete(vector, [0]), [value_temp])
        else:
            value_temp = vector[0:2]
            return np.append(np.delete(vector, [0, 1]), value_temp)

    print('###------------------------------------------------------------------###')
    print('# Bảng tính các khoá k')
    print(f'\tKhoá k: \n{input_k}')

    k_after_pc_1 = np.zeros((8, 7), dtype=int)
    index_pc_1 = matrix_index_pc_1()

    for row in range(k_after_pc_1.shape[0]):
        for col in range(k_after_pc_1.shape[1]):
            k_after_pc_1[row, col] = input_k.item(index_pc_1.item((row, col)) - 1)
    k_flatten = k_after_pc_1.flatten()

    print(f'\tSau khi hoán vị qua PC1, khoá k: \n{k_after_pc_1}')

    c0, d0 = k_flatten[:28], k_flatten[28:]

    print(f'\tC0: {c0}')
    print(f'\tD0: {d0}')

    index_pc_2 = matrix_index_pc_2()

    list_k = []

    for i in range(1, 17):
        print(f'\n\tDịch bit lần {i}')
        c0, d0 = ls_vector(c0, i), ls_vector(d0, i)
        print(f'\t\tC0: {c0}')
        print(f'\t\tD0: {d0}')
        k_after_ls = np.concatenate((c0, d0), axis=0).reshape((8, 7))
        k_after_pc_2 = np.zeros((8, 6), dtype=int)
        for row in range(k_after_pc_2.shape[0]):
            for col in range(k_after_pc_2.shape[1]):
                k_after_pc_2[row, col] = k_after_ls.item(index_pc_2.item((row, col)) - 1)
        print(f'\t\tKhoá k{i}: \n{k_after_pc_2}')
        list_k.append(k_after_pc_2)
    print('###------------------------------------------------------------------###')
    return list_k


def get_s_boxes():
    return [
        np.array([[14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7],
                  [0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8],
                  [4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0],
                  [15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13]]),
        np.array([[15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10],
                  [3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5],
                  [0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15],
                  [13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9]]),
        np.array([[10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8],
                  [13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1],
                  [13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7],
                  [1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12]]),
        np.array([[7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15],
                  [13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9],
                  [10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4],
                  [3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14]]),
        np.array([[2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9],
                  [14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6],
                  [4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14],
                  [11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3]]),
        np.array([[12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 15, 11],
                  [10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8],
                  [9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6],
                  [4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13]]),
        np.array([[4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1],
                  [13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6],
                  [1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2],
                  [6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12]]),
        np.array([[13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7],
                  [1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2],
                  [7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8],
                  [2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11]])
    ]


def p_index():
    return np.array([[16, 7, 20, 21],
                     [29, 12, 28, 17],
                     [1, 15, 23, 26],
                     [5, 18, 31, 10],
                     [2, 8, 24, 14],
                     [32, 27, 3, 9],
                     [19, 13, 30, 6],
                     [22, 11, 4, 25]])


def ip_inverse_index():
    x = np.array([[i for i in range(1, 65)]]).reshape((8, 8))
    x_0 = np.zeros(x.shape, dtype=int)
    x_0[:, 0] = x[4]
    x_0[:, 1] = x[0]
    x_0[:, 2] = x[5]
    x_0[:, 3] = x[1]
    x_0[:, 4] = x[6]
    x_0[:, 5] = x[2]
    x_0[:, 6] = x[7]
    x_0[:, 7] = x[3]
    return x_0[::-1, ::1]