
import bottle
import lmdb
import json
import datetime


env = lmdb.Environment("./dbreserve")

def get_id(txn): #keyが重複しないようにする　別にいらない(?)。キーとヴァリューを設定すればうまくいくみたい。キーだけで。現状、あまりいじらない。
    cur = txn.cursor()
    ite = cur.iterprev()
    try:
        k, v = next(ite)
        last_id = int(k.decode("utf8"))
    except StopIteration:
        last_id = 0
    id = last_id + 1
    return "{:08d}".format(id)
"""
@bottle.route("/")
@bottle.view("/")
def root():
    return bottle.template("entry.tpl", root="./static")
"""
@bottle.route("/static/<path:path>")
def static(path):
    return bottle.static_file(path, root="./static")

@bottle.post("/newaccount")
@bottle.view("newaccount")
def submit():
    name = bottle.request.params.name
    password = bottle.request.params.password
    accountdata = {"name": name, "password": password,"room":None}#1周目で/doreserveの中で、d["room"]==の式を成り立たせるためにroom:Noneを入れている。ちなみにNoneだから何も書き込まれていない１周目ではelseに入るはず。
    with env.begin(write=True) as txn1: #lmdbに書き込んでいる txn1にはアカウント情報を入れる
        id = get_id(txn1)
        txn1.put(id.encode("utf8"), json.dumps(accountdata).encode("utf8"))
    return accountdata

#name,passwordは新規アカウント作成時に入力されたもの（＝正解）。shimei,pastaはログイン画面で入力されたもの
@bottle.post("/login")
#@bottle.view("login")
def submit():
    shimei = bottle.request.params.shimei
    pasta = bottle.request.params.pasta
    logdata = {"shimei": shimei, "pasta": pasta}
    check =0 #毎回/loginに入るたびにcheckは初期化されるはず。既存のアカウントと、入力されたパスワードと氏名が一致しているか調べる用
    with env.begin() as txn1: #lmdb内に逐次アクセスしている
        cur = txn1.cursor()
        for k, v in cur:
            d = json.loads(v.decode("utf8")) 
            if d["password"]==logdata["pasta"] and d["name"]==logdata["shimei"]:
                check +=1
            else:
                print("アカウントにない入力がされた")
    if check>0: #アカウントにある情報が入力された場合
        dt_today1= datetime.date.today()#以下数行、カレンダーを１ヶ月に制限する用
        dt_raigetu1 = dt_today1 + datetime.timedelta(days=30)
        dt_today2=dt_today1.strftime('%Y-%m-%d') #datetime.dateっていうクラスから文字列に変換している
        dt_raigetu2=dt_raigetu1.strftime('%Y-%m-%d')
        data = [] #下にあるdataやtxnとはおそらく無関係。list()とsubmit()で、違う関数だから
        with env.begin() as txn1:#lmdb内に逐次アクセスしている。これはいるのか？
            cur = txn1.cursor()
            for k, v in cur:
                d = json.loads(v.decode("utf8")) #dにはjson形式でvalueが入っている　load関数はデコード
                data.append(d) 
        
        return bottle.template("entry.tpl",dt_today=dt_today2,dt_raigetu=dt_raigetu2,name=logdata["shimei"])
        #return bottle.template("entry.tpl",logdata)
    else: #ログイン画面で入力されたものが不正だった場合
        return bottle.template("login_error.tpl",cause="氏名かパスワードが間違っている")



dt_today1= datetime.date.today()#以下数行、カレンダーを１ヶ月に制限する用
dt_raigetu1 = dt_today1 + datetime.timedelta(days=30)
dt_today2=dt_today1.strftime('%Y-%m-%d') #datetime.dateっていうクラスから文字列に変換している
dt_raigetu2=dt_raigetu1.strftime('%Y-%m-%d')

@bottle.route("/entry")
@bottle.view("entry")
def root():
    data = [] #下にあるdataやtxnとはおそらく無関係。list()とsubmit()で、違う関数だから
    with env.begin() as txn1:#lmdb内に逐次アクセスしている。これはいるのか？
        cur = txn1.cursor()
        for k, v in cur:
            d = json.loads(v.decode("utf8")) #dにはjson形式でvalueが入っている　load関数はデコード
            data.append(d) 
    print(dt_today2)#デバッグ
    l_name1=[d.get('name')for d in data] #data内の,nameの値を取り出してリストにする
    print("登録完了ボタンから戻ってきた場合")
    for d in data: 
        print(data)
    print(l_name1)
    return bottle.template("entry.tpl",dt_today=dt_today2,dt_raigetu=dt_raigetu2,name=l_name1[0])


@bottle.post("/doreserve")
#@bottle.view("doreserve") #"doreserve"に入る直前に、viewの下で定義されている関数が開始、その後、viewの引数が表示
def submit():
    room = bottle.request.params.room
    #name = bottle.request.params.name
    purpose = bottle.request.params.purpose
    #year = bottle.request.params.year
    #month = bottle.request.params.month
    #day = bottle.request.params.day
    datex = bottle.request.params.datex
    time_hour = bottle.request.params.time_hour
    time_min = bottle.request.params.time_min
    use_time = bottle.request.params.use_time
    sep='-'
    s=datex.split(sep)#カレンダーからの入力を使いやすい形に変形する
    year=s[0]
    month=s[1]
    day=s[2]
    data = {"room":room, "year": year,  "month":month, "day":day,
            "time_hour":time_hour, "time_min":time_min,"use_time":use_time,"purpose": purpose,"password":None}#/loginでd["password"]で判定するからそのためにNone入れた
    #,"purpose": purpose, "name": name}
    #data = {"room":room,"datex":datex, "hour":hour, "purpose": purpose, "name": name}

    count = 0 #重複エラーを感知する用
    print("1回目{0}".format(count))
    #dt=[]
    with env.begin() as txn1: #lmdb内を逐次アクセス txn2は予約情報を入れる
        cur = txn1.cursor()
        for k, v in cur:
            d = json.loads(v.decode("utf8")) 
            #dt.append(d1)
            #for d in dt:
            if d["room"]==data['room'] and d["year"]==data['year']  and d["month"]==data['month']  and d["day"]==data['day']  and d["time_hour"]==data['time_hour'] and d["time_min"]==data['time_min']:
            #d["room"]==data['room'] and d["datex"]==data['datex']  and d["hour"]==data['hour'] 
                count +=1
                #print("T")
                print("2回目{0}".format(count))
            else:
                print("重複エラーなし")#デバッグ
                print("2.5回目{0}".format(count))
    print("3回目{0}".format(count))
    if count>0:
        print("今からエラーテンプレートに向かう")
        print("4回目{0}".format(count))
        return bottle.template("double_error.tpl",cause="重複エラーの")
    else:
        with env.begin(write=True) as txn1:#lmdb内に書き込み
            id = get_id(txn1)
            print("idは"+id)#デバッグ
            txn1.put(id.encode("utf8"), json.dumps(data).encode("utf8")) #dumps関数はエンコード（データをJSON形式に変換する）
         #print(type(data)) デバッグ dataは辞書型
        print(data)
        print("5回目{0}".format(count))
        return bottle.template("doreserve.tpl",data)


@bottle.route("/list")
@bottle.view("list")
def list():
    count=0 #170行目あたりでカウントするため
    #kazu=-1 #175行目あたりでカウントするため
    data = [] #上の、41,43行目とかのdata、txnは51行目以降のものとは無関係。list()とsubmit()で、違う関数だから。　
    with env.begin() as txn1: #lmdb内に逐次アクセス
        cur = txn1.cursor()
        for k, v in cur:
            d = json.loads(v.decode("utf8")) #dにはjson形式でvalueが入っている　load関数はデコード
            data.append(d) #リストdataの中に、辞書dを格納　　右に書いたことは多分違う、、dataは辞書の辞書？dは辞書？
    print("今からdataの中身を見る")
    for d in data: #デバッグ用
        print(d)
    
    l_name=[d.get('name')for d in data] #data内の,nameの値を取り出してリストにする
    l_password=[d.get('password')for d in data]
    #print(l_name) #デバッグ用

    """
    for i in range(len(l_name1)): #l_nameで別の人がログインして予約するたびにカウントする
        if l_name1[i] != None:
            kazu+=1
    print(kazu)
    print("これがkazuです")
    
    l_name2=filter(None,l_name1)
    l_password2=filter(None,l_password1)
    l_name=list(l_name2)
    l_password=list(l_password2)
    """

    for d in data:
        if d["room"]==None:
            data.pop(count)
        count+=1
    
    for d in data:
        d["name"]=l_name[0]#dataっていうリストの１番目の辞書に、辞書型の要素"name":nameを追加
        d["password"]=l_password[0]#一つ上と同様
    print("デバッグします")
    print(data) #デバッグ用

    data3=sorted(data,key=lambda x:x["room"])#部屋によって順番を並び替える
    for d in data3:
        tstr=d["year"]+"-"+d["month"]+"-"+d["day"]+" "+d["time_hour"]+":"+d["time_min"]
        tdatetime=datetime.datetime.strptime(tstr,'%Y-%m-%d %H:%M')#データ型を文字列からdatetime.datetimeに変更
        period1=tdatetime+datetime.timedelta(minutes=int(d["use_time"]))#使用時間を足す
        #print(period1) #デバッグ
        #period=period1.isoformat(timespec='minutes')#秒を切り捨てる。だが、"T"が入ってしまう
        period=period1.replace(second=0)
        #period=datetime.datetime.strftime('%Y/%m/%d')
        #print(period) #デバッグ
        d['period']=period #リストdata3の中の各辞書に、キーとバリューをセットで追加
    #print(data3) #デバッグ
    data3A=[]
    data3B=[]
    data3C=[]
    for d in data3:
        if d["room"]=="会議室A":
            data3A.append(d)
        if d["room"]=="会議室B":
            data3B.append(d)
        if d["room"]=="会議室C":
            data3C.append(d)
    data4A=sorted(data3A,key=lambda x:x["period"])#終了時間によって順番を並び替える
    data4B=sorted(data3B,key=lambda x:x["period"])#終了時間によって順番を並び替える
    data4C=sorted(data3C,key=lambda x:x["period"])#終了時間によって順番を並び替える
    return {"data5A": data4A,"data5B": data4B,"data5C": data4C} #右のdata4っていう小さな塊が複数集まったのが、左のdata5にまとめられている

bottle.run()