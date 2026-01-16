Kullanici_Adi=[]
Parola=[]
while(True):
    print("Afet bilgi programına hoş geldiniz.")
    print("1-Giriş yap\n2-Kaydol\n3-Şifremi unuttum")
    secim1=input()
    if secim1=="1":
        k_adi=input("Kullanıcı adınız : ")
        sifre=input("Parolanız : ")      
        if k_adi==Kullanici_Adi[0] and sifre==Parola[0]:
            print("Giriş başarılı")
        else:
            print("Hatalı giriş.")
    elif secim1=="2":
        k_adi=input("Kullanıcı adınızı belirleyin : ")
        sifre=input("Şifrenizi belirleyin = ")
        if len(sifre)<4:
            print("Sifreniz adınız minumum 4 karakter olabilir")
            continue
        Kullanici_Adi.insert(0,k_adi)
        Parola.insert(0,sifre)
        print("Kayıt başarılı!")
    elif secim1=="3":
        k_adi=input("Kullanıcı adınız :  ")
        if k_adi==Kullanici_Adi[0]:
            print("Kayıt bulundu.")
            sifre=input("Yeni şifreniz : ")
            Parola.insert(0, sifre)
            print("Yeni şifreniz belirlendi.")
        else:
            print("Kayıt bulunamadı.")
            
        
        
            