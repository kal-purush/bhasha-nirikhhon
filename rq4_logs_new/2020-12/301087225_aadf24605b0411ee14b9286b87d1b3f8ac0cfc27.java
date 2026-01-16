import java.util.*;
class DaftarFilm {
    
    public static HashMap<Integer,String> judul  = new HashMap<>();
    public static HashMap<Integer,String> rilis = new HashMap<>();
    public static HashMap<Integer,String> durasi = new HashMap<>();
    public static HashMap<Integer,String> genre =  new HashMap<>();
    public static HashMap<Integer,String> cast = new HashMap<>();
    public static HashMap<Integer,String> sinopsis = new HashMap<>();
    public static void main(String[] args) {
        Scanner uy = new Scanner(System.in);
        String a = "";
        //avenger
        judul.put(1, "Avenger : Endgame");
        rilis.put(1, "24 April 2019");
        durasi.put(1, "3 jam 2 menit");
        genre.put(1, "Fantasy, Sci-Fi");
        cast.put(1, "Robert Downey Jr., Chris Evans, Chris Hemsworth");
        sinopsis.put(1, "Melanjutkan Avengers Infinity War, dimana kejadian setelah Thanos berhasil mendapatkan semua infinity stones dan memusnahkan 50% semua mahluk hidup di alam semesta. Akankah para Avengers berhasil mengalahkan Thanos?");
        //spiderman
        judul.put(2, "Spiderman : Far From Home");
        rilis.put(2, "3 juli 2019");
        durasi.put(2, "2 jam 9 menit");
        genre.put(2, "Fantasy ,Sci-Fi");
        cast.put(2, "Tom Holland, Zendaya, Samuel L. Jackson");
        sinopsis.put(2, "Peter Parker (Tom Holland) tengah mengunjungi Eropa untuk liburan panjang bersama temaan-temanya. Sayangnya , Parker tidak bisa dengan tenang menikmati liburannya, karena Nick Fury datang secara tiba-tiba di kamar hotelnya. Selama di Eropa pun Peter harus menghadapi banyak musuh mulai dari Hydro Man, Sandman dan Molten Man.");
        //venom
        judul.put(3, "Venom");
        rilis.put(3, "3 Oktober 2018");
        durasi.put(3, "2 jam 20 menit");
        genre.put(3, "Thriller ,Sci-Fi");
        cast.put(3, "Tom Hardy, Michelle Williams, Woody Harrelson");
        sinopsis.put(3, "Seorang jurnalis, Eddie Brock (Tom Hardy) ingin melakukan sebuah investasi kasus terhadap penemuan yang dipimpin oleh Dr. Carlton Drake (Riz Ahmed). Karena investigasinya, Eddie mengunjungi lab milik Dr. Calrlton Drake. Semuanya ditujukan untuk membuktikan bahwa Dr. Carlton Drake sedang sedang melakukan tindakan jahat menggunakan Symbiote.");
        //thor
        judul.put(4, "Thor : Ragnarok");
        rilis.put(4, "25 Oktober 2017");
        durasi.put(4, "2 jam 10 menit");
        genre.put(4, "Fantasy, Sci-Fi");
        cast.put(4, "Chris Hemsworth, Taika Waititi, Tessa Thompson");
        sinopsis.put(4, "Dipenjara, Thor yang hebat menemukan dirinya dalam sebuah kontes gladiator yang mematikan melawan Hulk, mantan sekutunya. Thor harus berjuang untuk bertahan hidup dan berpacu melawan waktu untuk mencegah Hela yang sangat kuat menghancurkan rumah dan peradaban Asgardian.?");
        
        boolean movie = true;
        while(movie){
            cetak(a);
            System.out.println("(a)Add \t(d)Detail \t(s)Search \t(r)Remove \t(e)Exit" );
            String choice = uy.next();
            if (choice.equals("a")) {
                int n = uy.nextInt();
                System.out.println();
                add(n);
            } else if (choice.equals("d")) {
                int n = uy.nextInt();
                System.out.println();
                detail(n);
            } else if (choice.equals("r")){
                int n = uy.nextInt();
                System.out.println();
                remove(n);
            } else if (choice.equals("e")){
                System.exit(0);
            } else if (choice.equals("s")){
                String search = uy.next();
                System.out.println("\nHasil Pencarian Dari " +search+ " : ");
                for (int i = 1; i < judul.size(); i++) {
                    if (judul.get(i).contains(search)){
                        System.out.print(judul.keySet().toArray()[i-1]+" ");
                        System.out.println(judul.get(judul.keySet().toArray()[i-1]));
                    } else {
                    System.out.println("Tidak Ada");
                    break;
                    }
                }
                System.out.println();
            }
        }     
    }

    //detail
    public static void detail(int a){
        System.out.println("Detail Film : ");
        System.out.println("Judul\t: " + judul.get(a));
        System.out.println("Rilis\t: "+ rilis.get(a));
        System.out.println("Durasi\t: " + durasi.get(a));
        System.out.println("Genre\t: " + genre.get(a));
        System.out.println("Cast\t: " + cast.get(a));
        System.out.println("Sinopsis\t: " + sinopsis.get(a));
        System.out.println();
    }
    public static void cetak(String a) {
        System.out.println("\nFavourite Movie");
        for (int i = 0; i < judul.size(); i++) {
            System.out.print(judul.keySet().toArray()[i]+" ");
            System.out.println(judul.get(judul.keySet().toArray()[i]));
            
        }
    }
    public static void remove(int a) {
        judul.remove(a);
        rilis.remove(a);
        durasi.remove(a);
        genre.remove(a);
        cast.remove(a);
        sinopsis.remove(a);
    }

        //add
        public static void add(int a){
            Scanner sc = new Scanner(System.in);
            System.out.print("Judul: ");
            String title = sc.nextLine();
            System.out.print("Rilis: ");
            String release = sc.nextLine();
            System.out.print("Durasi: ");
            String duration = sc.nextLine();
            System.out.print("Genre: ");
            String jenre = sc.nextLine();
            System.out.print("Sinopsis: ");
            String synopsis = sc.nextLine();
            System.out.print("Cast: ");
            String casting = sc.nextLine();
    
            judul.put(a, title);
            rilis.put(a, release);
            durasi.put(a, duration);
            genre.put(a, jenre);
            sinopsis.put(a, synopsis);
            cast.put(a, casting);
    }
}