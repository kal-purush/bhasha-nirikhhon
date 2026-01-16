import tools.utils as utils
import tools.constant as constant
import os


def factory_init_product_list():
    product_list = []
    for i in range(12):
        product_list.append(False)
    return product_list


def add_product(product_list, product_num):
    product_num = int(product_num) - 1
    product_list[product_num] = True
    return product_list


def build_cust_prod(data):
    cust_prod = {}
    # for line in data:
    del data[0]
    for line in data:
        line_list = line.split(',')
        if line_list[0] not in cust_prod:
            product_list = factory_init_product_list()
            cust_prod[line_list[0]] = add_product(product_list, line_list[1])
        else:
            cust_prod[line_list[0]] = add_product(cust_prod[line_list[0]], line_list[1])
    return cust_prod


def build_write_file_head():
    product_list = []
    product_list.append('Customer_ID')
    for i in range(12):
        num = str(i + 1)
        product_list.append('Product_' + num.zfill(2))
    return utils.list_to_str(product_list)


def build_write_list(cust_prod_dic):
    write_list = []
    write_list.append(build_write_file_head())
    customer_list = list(cust_prod_dic.keys())
    customer_list.sort()
    for customer in iter(customer_list):
        line = []
        line.append(customer)
        line.extend(cust_prod_dic[customer])
        write_list.append(utils.list_to_str(line))
    return write_list


if __name__ == '__main__':
    print('start')
    data = utils.read_file(os.path.join(constant.data_files_path, constant.products_dat_name))
    cust_prod = build_cust_prod(data)
    cust_prod_write_list = build_write_list(cust_prod)
    utils.write_file('cust_prod_test.dat', cust_prod_write_list)
    print("end")