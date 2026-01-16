import os
x = len(os.listdir('./input_kv_msau/'))
print(x)
with open('train.lst', 'w', encoding='utf-8') as f:
    for i, file_name in enumerate(os.listdir('./input_kv_msau')):
        if i <= 120:
            f.write('./data_ibm_train_kv/input_kv_msau/' +file_name + '\n')

with open('val.lst', 'w', encoding='utf-8') as f:
    for i, file_name in enumerate(os.listdir('./input_kv_msau')):
        if i > 120:
            f.write('./data_ibm_train_kv/input_kv_msau/' + file_name + '\n')

with open('charset.txt', 'r', encoding='utf-8') as f:
    print(len(f.read()))
