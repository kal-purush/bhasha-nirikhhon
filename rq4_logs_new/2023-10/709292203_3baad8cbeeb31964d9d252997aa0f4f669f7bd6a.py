from flask import Flask, render_template, request, redirect, url_for, Response
import threading
import time

app = Flask(__name__)

# város lekérdezése után a város integrálása változóba
@app.route('/varosadat', methods=['GET', 'POST'])
def varosadat():
    varos = ''
    if request.method == 'POST':
        varos = request.form['inp']
        return redirect(url_for('index', varos=varos))

# napidő egyenletek
@app.route('/', methods=['GET', 'POST'])
def index():
    varos = request.args.get('varos')
    import math
    from datetime import datetime
    import ssl
    import certifi
    import geopy.geocoders
    import time
    import pytz
    from timezonefinder import TimezoneFinder
    ##
    context = ssl.create_default_context(cafile=certifi.where())
    geolocator = geopy.geocoders.Nominatim(user_agent="suntime-st", ssl_context=context)
    location = geolocator.geocode(varos)
    print("A GPS és zónaidő alapján meghatározott napidő\n") 
    Hónap=0
    Hónap=[31,28,31,30,31,30,31,31,30,31,30,31]
    Ny=1; # nyári időszámítás kikapcsolása
    K=location.longitude; #Keleti hosszúság
    É=location.latitude; #ÉSzaki szélesség
    Eszak=location.latitude  
    n=0; #Zónaidő-sorszám
    z=0; #zónaidő középérték helye
    delta=0; # zónaidő középérték helye
    st=0; # módosult idő
    FÉ=0; # északi szélesség a 4 perc torzulás
    Ó=0; #helyi zónaidő órája
    P=0; #helyi zónaidő perce
    #K=input("Adja meg a Keleti hosszúságot! (K)");
    #É=input("Adja meg az Északi szélességet! (É)");
    FÉ=240
    #Ny=input("Nyári időszámítás van? (I=1/N=0)");
    if varos:
        tf = TimezoneFinder()
        timezone_str = tf.timezone_at(lng=K, lat=É)
        idozona = pytz.timezone(timezone_str)
        now = datetime.now(idozona)
        print(idozona)
        print(now)
        n_string = str(now.strftime('%z'))
        if n_string[0] == "+":
            n = int(n_string[1:3])-1
        elif n_string[0] == "-":
            n = -int(n_string[1:3])-1
        z=int(n)*15
        delta=K-z
        print(n_string)
        print(n)
        print(z)
    else:
        idozona = ""
        now = datetime.now()
    time = [now.year, now.month, now.day, now.hour, now.minute, now.second]
    Y=time[0] #év lekérdezése számítógépről
    H=time[1] #hónap lekérdezése számítógépről
    N=time[2] #nap lekérdezése számítógépről
    Ó=time[3] #óra lekérdezése számítógépről
    P=time[4] #perc lekérdezése számítógépről
    M=time[5] #másodperc lekérdezése számítógépről
    #Ó=input("adja meg csak az órát! (0-23)");
    #P=input("adja meg csak az percet! (0-59)");

    if Ny==1:
        Ó=Ó-1
    if Ó<0:
        Ó=23#nehogy negatív órát kapjunk, pl.0:xx percnél
    # else if Ny==0 then Ó=Ó-0
    # idő tizedesbe váltása

    P1=P/60
    i=Ó+P1
    st=i+FÉ*delta/3600
    print("A napidő:            ");
    óra=int(st)
    perc=(st-óra)*60
    if óra == 24:
        óra = 0
    print(óra)
    print("{}:{:02d}".format(óra, int(perc)));
    #H=input("Hányadik hónap van? (1-12)");

    #N=input("Hányadik nap van? (1-31)");
    i1 = 0
    add = 0

    while i1 < H - 1:
        i1 = i1 + 1
        add = add + Hónap[i1-1]

    sorszam=add+N
    print(f"A dátum sorszáma:\n{sorszam}")
    B=2*3.14159*(sorszam-81)/364
    E=9.87*math.sin(2*B)-7.53*math.cos(B)-1.5*math.sin(B)# előjeles eltérés percekben
    st=i+FÉ*delta/3600+E/60
    print("A módosult napidő:   ");
    óra=int(st)
    perc=(st-óra)*60
    if óra == 24:
       óra = 0
    if M < 10:
        masodperc = str("0")+str(M)
    else:
        masodperc = str(M)
    print("{}:{:02d}:{}".format(óra, int(perc), masodperc))
    # delelés kiszámolása
    c=i-st
    d=c+12+Ny
    ód=int(d)
    pd=int((d-ód)*60)

    c_nap=c
    d_nap=d
    ód_nap=ód
    pd_nap=pd
    print("A delelési idő:      ");
    print("{}:{:02d}".format(ód, pd))
    #a delelési idők az év folyamán
    E=0;st=0;B=0
    x = range(1, 366)
    B = [2 * math.pi * (i - 81) / 364 for i in x]
    E = [9.87 * math.sin(2 * b) - 7.53 * math.cos(b) - 1.5 * math.sin(b) for b in B] # előjeles eltérés percekben

    delta = -23.44 * math.cos(2 * math.pi * (284 + 10) / 365.25)  # Nap dőlésszöge
    st = [i + FÉ * delta / 3600 + e / 60 for i, e in zip(x, E)]
    c = [i - s for i, s in zip(x, st)]
    Ny = 1  # Nyári időszámítás (óra)
    d = [ci + 12 + Ny for ci in c]
    #deklináció kiszámítása
    Delta=-23.44*math.cos((360/365*(sorszam+10))*math.pi/180)
    #delelési magasság kiszámolása
    DM=90-Eszak+Delta
    print("A delelési magasság: \n%.3f" % DM);#%.3f\n 3 tizedesjegy, d/n egész szám
    # Nap hosszának kiszámítása
    Nap = 2/15 * math.acos(-math.tan(É*math.pi/180) * math.tan(Delta*math.pi/180)) * (180/math.pi)
    # Napfelkelte kiszámítása
    nf = d_nap - Nap/2
    ónf = int(nf)
    pnf = int((nf-ónf)*60)
    print("A napfelkelte:\n{:02d}:{:02d}".format(ónf, pnf))
    # Napnyugta kiszámítása
    nny = d_nap + Nap/2
    ónny = int(nny)
    pnny = int((nny-ónny)*60)
    print("A napnyugta:\n{:02d}:{:02d}".format(ónny, pnny))
    # Nap hossza
    print("A nap hossza:\n{:.3f}".format(Nap))

    # HTML oldalra való változóbeágyazás
    napido = "{}:{:02d}".format(óra, int(perc))
    # sorszam változó már létre van hozva
    mod_napido = ("{}:{:02d}:{}".format(óra, int(perc), masodperc))
    del_ido = "{}:{:02d}".format(ód, pd)
    del_mag = "%.3f°" % DM
    masodperc_int = int(masodperc)
    perc_int = int(perc)
    napfelkelte = "{:02d}:{:02d}".format(ónf, pnf)
    napnyugta = "{:02d}:{:02d}".format(ónny, pnny)
    nap_hossz_ora = int(Nap)
    nap_hossz_perc = int((Nap - nap_hossz_ora) * 60)
    nap_hossz = str(nap_hossz_ora) + " óra " + str(nap_hossz_perc) + " perc"
    felkelte_ora = int(ónf)
    felkelte_perc = int(pnf)
    nyugta_ora = int(ónny)
    nyugta_perc = int(pnny)

    # ha nincs megadva város
    if varos:
        varos_adatok = varos.split(",")
        varos_kiiratas = ",".join(varos_adatok[:2])
        print("Város sikeresen megadva!")
    else:
        napido = "--:--"
        sorszam = "--"
        mod_napido = "--:--"
        del_ido = "--:--"
        del_mag = "--°"
        varos = "Nincs megadva"
        óra = "--"
        perc_int = "--"
        masodperc_int = "--"
        É = "--"
        K = "--"
        varos_kiiratas = "Adjon meg egy települést vagy városrészt"
        napfelkelte = "--:--"
        napnyugta = "--:--"
        nap_hossz = "-- óra -- perc"

    return render_template('index.html',napido=napido, varos=varos_kiiratas, sorszam=sorszam, mod_napido=mod_napido, del_ido=del_ido, del_mag=del_mag, ora=óra, perc=perc_int, masodperc=masodperc_int, szel=Eszak, hossz=K, napfelkelte=napfelkelte, napnyugta=napnyugta, nap_hossz=nap_hossz, idozona=idozona, felkelte_ora=felkelte_ora, felkelte_perc=felkelte_perc, nyugta_ora=nyugta_ora, nyugta_perc=nyugta_perc)

# ha nem létezik olyan város, amit megadott a felhasználó
@app.errorhandler(500)
def internal_server_error(error):
    varos_kiiratas = "Nincsen ilyen település vagy városrész"
    É = "--"
    K = "--"
    del_ido = "--:--"
    del_mag = "--°"
    sorszam = "--"
    napfelkelte = "--:--"
    napnyugta = "--:--"
    nap_hossz = "-- óra -- perc"
    return render_template('index.html',varos=varos_kiiratas, szel=É, hossz=K, del_ido=del_ido, del_mag=del_mag, sorszam=sorszam, napfelkelte=napfelkelte, napnyugta=napnyugta, nap_hossz=nap_hossz), 500

# webszerver indítása
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)