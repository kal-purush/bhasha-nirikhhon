public class BangunDatar {
    double sisi1, sisi2;
    int jumlahsisi;
    double sisi(){
        return 0;
    }
    double luas(double sisi1,double sisi2){
        return sisi1*sisi2;
    }
    double keliling(double sisi1, int jumlahsisi2, double sisi2){
        return jumlahsisi2 * (sisi1 + sisi2);
    }
}
class Persegi extends BangunDatar{
    int jumlahsisi = 2;
    double luas(double sisi1, double sisi2){
        return super.luas(sisi1,sisi2);
    }
    double Keliling(double sisi1){
        return super.keliling(sisi1, jumlahsisi, sisi2) ;
    }
}
class Lingkaran extends BangunDatar{
    int jumlahsisi = 1;
    double keliling(double sisi){
        return  Math.PI * super.keliling(sisi,jumlahsisi, sisi);
    }
    double luas(double sisi){
        return  Math.PI * super.luas(sisi, sisi);
    }
}
class PersegiPanjang extends BangunDatar{
    int jumlahsisi = 2;
    double luas(double sisi1, double sisi2){
        return super.luas(sisi1, sisi2) ;
    }
    double Keliling(double sisi1, double sisi2){
        return super.keliling(sisi1, jumlahsisi, sisi2) ;
    }
}
class Trapesium extends BangunDatar{
    int jumlahsisi = 4;
    double luas(double sisi1, double sisi2, double t){
        double a = ( sisi1 + sisi2) / 2 ;
        double b = t;
        return super.luas (a , b);
    }
    double keliling(double sisi1, double sisi2, double sisi3, double sisi4){
        double a = (sisi1 + sisi3)/4; 
        double b = (sisi2 + sisi4)/4;
        return super.keliling(a,jumlahsisi,b);
    } 
}

