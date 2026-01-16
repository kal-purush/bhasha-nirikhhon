# Author: Jon
# -*- coding: UTF-8 -*-
import json
f_name = 'productions.json'
productions = {}
# 写入文件操作
def p_write(production):
    with open(f_name, 'w') as f_obj:
        json.dump(productions, f_obj)

while True:
        msg = """ \t\t1、Enter a add items..             
        2、Enter c check items..
        3、Enter m modify price.. 
        4、Enter d delete items..
        5、Enter q quit..
        """                                             # 结构化输出
        print(msg.rstrip())
        choice = input(" Please enter your choice >>".lstrip())
        # 添加商品
        if choice == 'a':
            while True:
                add_item = input("Enter a add item >>".lstrip())
                if add_item == 'b':
                    break
                add_price = input("Enter add item price >>".lstrip())
                if add_price.isdigit():
                    add_price = int(add_price)
                    productions[add_item] = add_price
                    p_write(productions)
                    print("%s already  added into production.." % add_item.capitalize())
                print("Enter b back..")
            choice = 'EOF'                              # 避免无法跳出循环
        # 查看商品
        elif choice == 'c':
            try:                                        # json读取空文件异常处理
                with open(f_name) as f_obj:             # 读取文件内容，无商品必须添加
                    productions = json.load(f_obj)
            except json.decoder.JSONDecodeError:
                print("please add item..".capitalize())
            else:
                print('编号 商品名 单价\n'.rstrip())
                for num, item in enumerate(productions):
                    print(num, "\t", item, '\t', productions[item])
        # 删除商品
        elif choice == 'd':
            del_item = input("Please enter will to delete item >>".lstrip())
            with open(f_name) as f_obj:
                productions = json.load(f_obj)
            if productions.get(del_item):
                del productions[del_item]
                p_write(productions)
                print("%s deleted .." % del_item)
            else:
                print('Enter Invalid')
        # 修改商品
        elif choice == 'm':
            modify_item = input("Please enter will to modify item >>".lstrip())
            modify_price = input("Please enter price >>".lstrip())
            if modify_price.isdigit():                   # 商品价格为数值型数据
                with open(f_name) as f_obj:
                    productions = json.load(f_obj)
                if modify_item in productions:
                    productions[modify_item] = modify_price
                    p_write(productions)
                else:
                    print('Enter Invalid')
        elif choice == 'q':
            exit()