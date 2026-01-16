class Ganteng {
    constructor(apakahganteng, rambut, sixpack, motornyaapa){
        this.apakahganteng = apakahganteng;
        this.rambut = rambut;
        this.sixpack = sixpack;
        this.motornyaapa = motornyaapa;
    }

    makanBanyak() {
        this.sixpack = false;
    }

    kenaBegal() {
        this.motornyaapa = 'mio';
    }
}

class BoyAnakJalanan extends Ganteng {
    constructor(apakahganteng, rambut, sixpack, motornyaapa) {
        super(apakahganteng, rambut, sixpack, motornyaapa);
        this.apakahMati = true;
    }

    ibuibuKomplain() {
        this.apakahMati = false;
    }
}

class Saya extends Ganteng {
    constructor(apakahganteng, rambut, sixpack, motornyaapa) {
        super(apakahganteng, rambut, sixpack, motornyaapa);
    }

    kuliahDiITB() {
        this.apakahganteng = false;
        this.rambut = 'acak-acakan';
        this.sixpack = false;
        this.motornyaapa= 'Ninja kok. masih belom kena begal aja.';
    }
}

const siBoyAnakJalanan = new BoyAnakJalanan(true, 'Afro', true, 'Ninja')
const siSaya = new Saya(true, 'Afro', true, 'Ninja')

console.log('Berita terbaru! Boy si anak jalanan dinyatakan mati: ' + siBoyAnakJalanan.apakahMati);
console.log('HAH MASA SI BOY MATI??!!');
siBoyAnakJalanan.ibuibuKomplain();
console.log('Berita terbaru! Boy si anak jalanan dinyatakan mati: ' + siBoyAnakJalanan.apakahMati);

console.log('motor boy skrg adalah: ' + siBoyAnakJalanan.motornyaapa)
console.log('WOI SERAHIN MOTOR LO. INI BEGAL. PILIH NYAWA ATO MOTOR?')
siBoyAnakJalanan.kenaBegal();
console.log('motor boy skrg adalah: ' + siBoyAnakJalanan.motornyaapa)

console.log('hai, nama saya james, ini profilku:')
console.log(siSaya)
console.log('namun semua berubah ketika kuliah menyerang. ini profilku sekarang: ')
siSaya.kuliahDiITB();
console.log(siSaya)