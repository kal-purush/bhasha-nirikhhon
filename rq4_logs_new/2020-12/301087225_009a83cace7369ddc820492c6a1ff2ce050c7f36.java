import java.util.HashMap;
import java.util.Scanner;

class Pratikum7 {

    public static HashMap<Integer, String> judul = new HashMap<>();
    public static HashMap<Integer, String> rilis = new HashMap<>();
    public static HashMap<Integer, String> durasi = new HashMap<>();
    public static HashMap<Integer, String> genre = new HashMap<>();
    public static HashMap<Integer, String> cast = new HashMap<>();
    public static HashMap<Integer, String> sinopsis = new HashMap<>();

    public static void main(String[] args) {
        judul.put(1, "Avenger : Endgame");
        rilis.put(1, "24 April 2019");
        durasi.put(1, "3 jam 2 menit");
        genre.put(1, "Fantasy, Sci-Fi");
        cast.put(1, "Robert Downey Jr., Chris Evans, Chris Hemsworth");
        sinopsis.put(1,
                "Melanjutkan Avengers Infinity War, dimana kejadian setelah Thanos berhasil mendapatkan semua infinity stones dan memusnahkan 50% semua mahluk hidup di alam semesta. Akankah para Avengers berhasil mengalahkan Thanos?");

        judul.put(2, "Spiderman : Far From Home");
        rilis.put(2, "3 juli 2019");
        durasi.put(2, "2 jam 9 menit");
        genre.put(2, "Fantasy ,Sci-Fi");
        cast.put(2, "Tom Holland, Zendaya, Samuel L. Jackson");
        sinopsis.put(2,
                "Peter Parker (Tom Holland) tengah mengunjungi Eropa untuk liburan panjang bersama temaan-temanya. Sayangnya , Parker tidak bisa dengan tenang menikmati liburannya, karena Nick Fury datang secara tiba-tiba di kamar hotelnya. Selama di Eropa pun Peter harus menghadapi banyak musuh mulai dari Hydro Man, Sandman dan Molten Man.");

        judul.put(3, "Venom");
        rilis.put(3, "3 Oktober 2018");
        durasi.put(3, "2 jam 20 menit");
        genre.put(3, "Thriller ,Sci-Fi");
        cast.put(3, "Tom Hardy, Michelle Williams, Woody Harrelson");
        sinopsis.put(3,
                "Seorang jurnalis, Eddie Brock (Tom Hardy) ingin melakukan sebuah investasi kasus terhadap penemuan yang dipimpin oleh Dr. Carlton Drake (Riz Ahmed). Karena investigasinya, Eddie mengunjungi lab milik Dr. Calrlton Drake. Semuanya ditujukan untuk membuktikan bahwa Dr. Carlton Drake sedang sedang melakukan tindakan jahat menggunakan Symbiote.");

        judul.put(4, "Thor : Ragnarok");
        rilis.put(4, "25 Oktober 2017");
        durasi.put(4, "2 jam 10 menit");
        genre.put(4, "Fantasy, Sci-Fi");
        cast.put(4, "Chris Hemsworth, Taika Waititi, Tessa Thompson");
        sinopsis.put(4,
                "Dipenjara, Thor yang hebat menemukan dirinya dalam sebuah kontes gladiator yang mematikan melawan Hulk, mantan sekutunya. Thor harus berjuang untuk bertahan hidup dan berpacu melawan waktu untuk mencegah Hela yang sangat kuat menghancurkan rumah dan peradaban Asgardian.?");

        boolean a = true;
        Scanner read = new Scanner(System.in);
        while (a) {
            String movie = "";
            cetak(movie);
            System.out.println("(d)Detail (s)Search (a)Add (r)Remove");
            String pilihan = read.next();
            if (pilihan.equals("a")) {
                int x = judul.size() + 1;
                System.out.println();
                add(x);
            } else if (pilihan.equals("d")) {
                int x = read.nextInt();
                System.out.println();
                detail(x);
            } else if (pilihan.equals("r")) {
                int x = read.nextInt();
                System.out.println();
                remove(x);
            } else if (pilihan.equals("s")) {
                String search = read.next();
                System.out.println("\nHasil Pencarian Dari " + search + " : ");
                for (int i = 1; i <= judul.size(); i++) {
                    if (judul.get(i).contains(search)) {
                        detail(i);
                        break;
                    } else if (i == judul.size()) {
                        System.out.println("Tidak Ada");
                    }
                }
                System.out.println();
            } else {
                System.out.println("Inputan harus berupa satu huruf yang tersedia");
                System.out.println("Contoh : Input s untuk melakukan search");
            }
        }
    }

    public static void cetak(String movie) {
        System.out.println("\nFavourite Movie");
        for (int i = 1; i <= judul.size(); i++) {
            System.out.printf("%s. %s\n", i, judul.get(i));
        }
    }

    public static void detail(int movie) {
        System.out.println("Detail Movie : ");
        System.out.println("Judul   : " + judul.get(movie));
        System.out.println("Rilis   : " + rilis.get(movie));
        System.out.println("Durasi  : " + durasi.get(movie));
        System.out.println("Genre   : " + genre.get(movie));
        System.out.println("Cast    : " + cast.get(movie));
        System.out.println("Sinopsis: " + sinopsis.get(movie));
        System.out.println();
    }

    public static void add(int movie) {
        Scanner baru = new Scanner(System.in);
        System.out.print("Judul     : ");
        String judulbaru = baru.nextLine();
        System.out.print("Rilis     : ");
        String rilisbaru = baru.nextLine();
        System.out.print("Durasi    : ");
        String durasibaru = baru.nextLine();
        System.out.print("Genre     : ");
        String genrebaru = baru.nextLine();
        System.out.print("Sinopsis  : ");
        String sinopsisbaru = baru.nextLine();
        System.out.print("Cast      : ");
        String castbaru = baru.nextLine();

        judul.put(movie, judulbaru);
        rilis.put(movie, rilisbaru);
        durasi.put(movie, durasibaru);
        genre.put(movie, genrebaru);
        sinopsis.put(movie, sinopsisbaru);
        cast.put(movie, castbaru);
    }

    public static void remove(int movie) {
        for (int i = movie; i < judul.size(); i++) {
            judul.replace(i, judul.get(i + 1));
            rilis.replace(i, rilis.get(i + 1));
            durasi.replace(i, durasi.get(i + 1));
            genre.replace(i, genre.get(i + 1));
            cast.replace(i, cast.get(i + 1));
            sinopsis.replace(movie, sinopsis.get(movie + 1));
        }
        if (movie <= judul.size()) {
            judul.remove(judul.size());
            rilis.remove(judul.size());
            durasi.remove(judul.size());
            genre.remove(judul.size());
            cast.remove(judul.size());
            sinopsis.remove(judul.size());
        }
    }
}