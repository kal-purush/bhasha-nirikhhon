/*
 * Inheritance (Kalıtım | Miras):
 * Bir OOP kavramıdır bir sınıfın farklı bir özelliklerini
 * methodlarını miras almasına izin verir.
 
 * Bu işlem kodun tekrar kullanıalbilirğini arttırır ve
 * kod organizasyonunu geliştirir. Miras genellikle "anasınıf"
 * | "üst sınıf" 'tan türetilmiş olan alt sınıflar rasında geçekleşir.
 
 * Üst Sınıf (Base | Parent Class): Genel özellikleri ve
 * davranışlara sahip olan. Farklı sınıflar tarafından miras
 * alınabilen class'lara verdiğimiz isim
 
 * Alt Sınıf (Derived | Child Class): Bir üst sınıftan miras alarak
 * özelliklerini ve davranışlarını genişletebilen. Üst sınıftan türetilir.
 */

//! Parent Class
class GeometrikSekil {
  constructor(public ad: string) {}

  // ortak bir method
  tanim() {
    console.log(`Bu bir ${this.ad} geometrik şeklidir.`);
  }
}

//! Child Class
class Dikdortgen extends GeometrikSekil {
  private uzunluk: number;
  private genislik: number;

  constructor(yeni_uzunluk: number, yeni_genislik: number) {
    // üst sınıfın contructor'ını çağırma
    super('Dikdörtgen');

    this.uzunluk = yeni_uzunluk;
    this.genislik = yeni_genislik;
  }

  //  üst sınıfın methodlarınan bağımzsız yenilerini ekleyebiliyoruz
  alanHesapla(): number {
    console.log(this.uzunluk * this.genislik);
    return this.uzunluk * this.genislik;
  }

  // method override > üst sınıfın methodunu güncellemeye yarar
  tanim(): void {
    super.tanim(); // kapsyaıcıdaki methodu miras alır
    console.log('selam');
  }
}

const dik = new Dikdortgen(50, 100);
dik.tanim();
dik.alanHesapla();

//! 2. child class
class Daire extends GeometrikSekil {
  private yariCap: number;

  constructor(yeni_yaricap: number) {
    super('Daire');
    this.yariCap = yeni_yaricap;
  }

  alanHesapla() {
    return Math.PI * this.yariCap * this.yariCap;
  }
}

const daire = new Daire(6);
daire.tanim();
console.log(daire.alanHesapla());

// Birden fazla kalıtım

class Human {
  eye_color: string = '';
}

class Father extends Human {
  eye_color = 'kahve_rengi';
}

class Mother extends Human {
  eye_color = 'mavi';
}

class Son extends Father {}

const hasan = new Son();
console.log(hasan.eye_color);