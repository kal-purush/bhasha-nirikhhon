def select_case(FileName, n=10):
    import random   #Импортируем модуль random,
                    #Не разобрался если честно как удобнее использовать numpy
                    #Но, зато попробовал как новую библиотеку подключать
    
    NewFileName = 'all_pairs_res.txt'   #С твоего позволения пока не заморачиваюсь с rename файла
    s = []

    #Считаем количество строк в исходном файле
    count = 0
    with open(FileName) as f:
        for line in f:
            count += 1
            
    #Добавляем немного юмора, тестил на исходном файле текстовом в 70мб
    if count >= 100: print("Working... Don't reboot pls...")
    
    ch = random.sample(range(count), n) #Выбираем n рандомных номеров строк,
                                        #Позже добавлю контроль ошибки, когда выбираем больше строк чем в файле 

    with open(NewFileName, 'w') as g:
        with open(FileName) as f:
            for i,line in enumerate(f):
                if i in ch:             #Проверяем если номер строки попадает в "карту рандома ch"
                    g.write(line)       #Добавляю в файл выборки, иначе кидаю в s для последующей перезаписи исходного файла
                else:
                    s.append(line)

    with open(FileName, 'w') as f:      #Перезаписываю исходный файл
        f.writelines(s)