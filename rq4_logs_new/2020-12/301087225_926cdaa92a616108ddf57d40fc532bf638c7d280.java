import java.util.Scanner;
class Praktikum1{
    public static void main(String[] args) {
        Scanner n = new Scanner(System.in);
        System.out.println("program ini menampilkan kode unik");
        System.out.println("masukkan kata anda");
        String Kalimat = n.nextLine();
        //mangubah menjadi huruf besar semua
        String KalimatBaru = Kalimat.toUpperCase();
        //mengembalikan int dari panjang String
        //berapa panjang isinya kalimat baru atau berapa karakternya semua
        int panjang = KalimatBaru.length();
        //nilai dipangkat dua kemudian diubah ke int
        int hex = (int)Math.pow(panjang,2);
        //menampilkan angka hexa dari int hex
        String hexadecimal = String.format("%x", hex);
        //menghapus sepasi
        String baru = KalimatBaru.replace(" ","");
        //isi dari variabel baru dirubah menjadi array tapi dalam tipe data char 
        //dari karakternya itu sesuai dari variabel baru 
        char [] kata = baru.toCharArray();
        int panjangBaru = kata.length/2;

        char awal [] = new char[panjangBaru];
        for (int i = 0; i < panjangBaru; i++){
            //mengembalikan char pada indeks i
            awal[i] = baru.charAt(i);
        }
        //mengubah objek menjadi String awal
        String octal = String.valueOf(awal);
        //polindrom dimulai dari belakang
            String polindrom = "";
        //perulangan dimulai dari belakang jadi yang terbesar kemudian ke terkecil
            for (int i = octal.length()-2; i >= 0; i--){
                //karakter keberapa ke i nya
                polindrom = polindrom + baru.charAt(i);
            }
            //menampilkan String
            //menampilkan angka octal
            System.out.printf("#%s%s%s%o%s", hexadecimal,octal,polindrom,panjang,(int)hexadecimal.charAt(0) >= 65?"?":"!");

}
}