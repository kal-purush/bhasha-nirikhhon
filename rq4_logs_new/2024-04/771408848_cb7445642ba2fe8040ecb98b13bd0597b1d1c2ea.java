package TP5_1_H071231021_Simple;

class BangunDatar {
    double sisi;
    double panjang;
    double lebar;
    double radius;
    double tinggi;

    double hitungLuas() {
        return 0;
    }

    double hitungKeliling() {
        return 0;
    }
}

class Persegi extends BangunDatar {

    Persegi(double sisi) {
        this.sisi = sisi;
    }

    double hitungLuas() {
        return sisi * sisi;
    }

    double hitungKeliling() {
        return 4 * sisi;
    }
}

class PersegiPanjang extends BangunDatar {

    PersegiPanjang(double panjang, double lebar) {
        this.panjang = panjang;
        this.lebar = lebar;
    }

    double hitungLuas() {
        return panjang * lebar;
    }

    double hitungKeliling() {
        return 2 * (panjang + lebar);
    }
}

class Lingkaran extends BangunDatar {

    Lingkaran(double radius) {
        this.radius = radius;
    }

    double hitungLuas() {
        return Math.PI * radius * radius;
    }

    double hitungKeliling() {
        return 2 * Math.PI * radius;
    }
}

class Trapesium extends BangunDatar {
    double sisi1, sisi2, sisi3, sisi4;

    Trapesium(double sisi1, double sisi2, double sisi3, double sisi4, double tinggi) {
        this.sisi1 = sisi1;
        this.sisi2 = sisi2;
        this.sisi3 = sisi3;
        this.sisi4 = sisi4;
        this.tinggi = tinggi;
    }

    double hitungLuas() {
        return ((sisi1 + sisi2) * tinggi) / 2;
    }

    double hitungKeliling() {
        return sisi1 + sisi2 + sisi3 + sisi4;
    }
}