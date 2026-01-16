#########このファイルは中井が担当しました。コメントは自分用

# row_data = '''\
# [氏名],"中井 亮","","",[学籍番号],"1910453"
# [所属],"情報理工学域Ⅲ類","","",[年次],"1年 "
# [年度・学期],"2019年度・前学期",[期限],"登録期間外",[件数],"14件"

# "","","","[最終更新日時：2019/04/18 13:33]","",""

# "","月曜日","火曜日","水曜日","木曜日","金曜日","土曜日"
# "1限","未登録","21012134","未登録","21018101","未登録","未登録"
# "","","Academic Spoken English Ⅰ","","総合コミュニケーション科学 ","",""
# "","","MOREAU ROBERT","","由良 憲二","",""
# "2限","21018119","21019109","未登録","未登録","21012110","未登録"
# "","コンピュータリテラシー ","微分積分学第一","","","Academic Written EnglishⅠ",""
# "","西 康晴","久藤 衡介","","","HIGUCHI SONIA",""
# "3限","21012176","21019145","21019133","未登録","21018107","未登録"
# "","中国語第一","物理学概論第一","数学演習第一","","基礎科学実験B",""
# "","鷲巣 益美","來住 直人","伊藤 賢一","","小林 義男",""
# "4限","21014305","21019159","未登録","21019121","21018107","未登録"
# "","健康・体力つくり実習","化学概論第一","","線形代数学第一","基礎科学実験B",""
# "","大河原・○竹内・○八百・○青木","安井 正憲","","陸名 雄一","小林 義男",""
# "5限","未登録","未登録","未登録","未登録","未登録","未登録"
# "","","","","","",""
# "","","","","","",""
# "6限","未登録","未登録","未登録","未登録","未登録","未登録"
# "","","","","","",""
# "","","","","","",""
# "7限","未登録","未登録","未登録","未登録","未登録","未登録"
# "","","","","","",""
# "","","","","","",""

# [集中講義など]
# [曜日],[時限],[時間割コード],[科目],[担当教員名]
# "その他","その他","21124301","情報工学工房（学域）","工藤 俊亮"

# [履修中止一覧]
# 登録されていません。
# '''

def parse_csv(row_data):

    monday = []
    tuesday = []
    wednesday = []
    thursday = []
    friday = []
    saturday = []

    day_list = [monday, tuesday, wednesday, thursday, friday, saturday,]

    # model_dict = {
    # 'class_num' : 191984,
    # 'periods' : 1,
    # 'dayofweek' : 1,
    # 'teachers' : okawara,
    # 'subject' : computer_literacy,
    # }

    #day_listっていうリストがあるから下のべた書きになっているところはfor文でリファクタリングしたい。ただしなくてもほぼ問題なし
    #下のfor row in enter_sentence_list[]ってところは行番号で指定しているからなんかの都合で行がずれたら全部パー

    enter_sentence_list = row_data.split('\n')
    for row in enter_sentence_list[7:27]:
        columns = row.replace('\"', '').rstrip().split(',')
        monday.append(columns[1])
        tuesday.append(columns[2])
        wednesday.append(columns[3])
        thursday.append(columns[4])
        friday.append(columns[5])
        saturday.append(columns[6])

    day_count = 0
    result_list = []
    for day_of_week in day_list:

        #要素が３個になったときにリストを作る　一科目当たりの情報は３つでまとまっているため。
        #ele_listに要素を３つ溜めてele_list2を'3つの要素を持つリスト'を要素とするリストとする
        ele_list = []
        ele_list2 = []
        for element in day_of_week:
            ele_list.append(element)
            if len(ele_list) == 3:
                ele_list2.append(ele_list)
                ele_list = []               #ele_listを初期化

        #１個目のリストにはperiodに対して一限、二個目には二限という値を与える
        #リストの1個目はclassnum,二個目はsubject,三個目はteachers、という感じ
        #countは時限を取得するためのカウンター、day_countは曜日を取得するためのカウンター。
        ele_dict = {}
        count = 1
        for ele_list3 in ele_list2:
            #未登録の授業の時、result_listへの追加をスキップする
            if ele_list3[0] == '未登録':
                count += 1
                continue
            #同じ教科が連続している時、periodsリストを連番にする。
            #tmp_class_num(一個前の辞書要素)が現在のclass_numと一致するか検証
            #result_list[-1]は一個前の辞書要素。result_lits[-1]['periods']はその辞書の中に入ってる値のリスト
            if 'tmp_class_num' in locals() and tmp_class_num == ele_list3[0]:
                result_list[-1]['periods'].append(count)
                count += 1
                continue

            ele_dict = {'periods':[count],
            'class_num':ele_list3[0],
            'subject':ele_list3[1],
            'teachers':ele_list3[2],
            'dayofweek':day_count}
            result_list.append(ele_dict)
            tmp_class_num = ele_list3[0]
            count += 1
        
        day_count += 1
    return result_list

# print(parse_csv(row_data))