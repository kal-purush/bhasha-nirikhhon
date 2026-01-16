
import tools.utils as utils
import os
import tools.constant as constant

def merge(data1, data2):
    cust_call_prod = {}
    del data1[0]
    for line in data1:
        line_list = line.split(',')
        id = line_list[0]
        # cust_call_prod[id] = line_list[1:]
        for line1 in data2:
            line_list1 = line1.split(',')
            if line_list1[0] == id:
                cust_call_prod[id] = line_list[1:]
                cust_call_prod[id].extend(line_list1[1:])
                VulueBand = float(line_list[30])
                if VulueBand>=200 and VulueBand<250 :
                    cust_call_prod[id].append('medium high')
                elif VulueBand>=100 and VulueBand<150:
                    cust_call_prod[id].append('medium low')
                elif VulueBand>=150 and VulueBand<200:
                    cust_call_prod[id].append('medium')
                elif VulueBand<100:
                    cust_call_prod[id].append('low')
                # print(cust_call_prod[id])
    return cust_call_prod

def build_write_file_head():
    product_list = []
    for i in range(12):
        num = str(i + 1)
        product_list.append('Product_' + num.zfill(2))
    return utils.list_to_str(product_list)

def build_write_list(cust_prod_dic):
    write_list = []
    write_list.append(head)
    write_list.append(build_write_file_head())
    write_list.append('ValueBand')
    customer_list = list(cust_prod_dic.keys())
    customer_list.sort()
    for customer in iter(customer_list):
        line = []
        line.append(customer)
        line.extend(cust_prod_dic[customer])
        write_list.append(utils.list_to_str(line))
    return write_list


if __name__ == "__main__":
    print('start')
    # data1 = utils.read_file('D:\program\IBM\data_files\cust_call_plus.dat')
    data1 = utils.read_file(os.path.join(constant.data_files_path, constant.cust_name1))
    head = []
    head = data1[0]
    data2 = utils.read_file(os.path.join(constant.data_files_path, constant.cust_name2))
    cust_call_prod = merge(data1, data2)
    cust_prod_write_list = build_write_list(cust_call_prod)
    utils.write_file('cust_call_prod_test.dat', cust_prod_write_list)
    print('end')
