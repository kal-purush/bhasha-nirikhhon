import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;

public class Nomor1 {

    public static List<String> judul = new ArrayList<String>();
    public static List<String> rilis = new ArrayList<String>();
    public static List<String> durasi = new ArrayList<String>();
    public static List<String> genre = new ArrayList<String>();
    public static List<String> sinopsis = new ArrayList<String>();
    public static List<String> cast = new ArrayList<String>();

    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);

        judul.add("Avengers : End Game");
        rilis.add("24 April 2019");
        genre.add("Adventure, Sci-Fi, Action");
        sinopsis.add("ini adalah sinopsis");
        durasi.add("3 jam 1 Menit");
        cast.add("Robert Downey Jr., Chris Evan, Mark Ruffalo");

        judul.add("Spiderman : far from home");
        rilis.add("3 Juli 2019");
        genre.add("Adventure,Action");
        sinopsis.add("ini adalah sinopsis");
        durasi.add("2 Jam 9 Menit");
        cast.add("Tom Holland,Samuel L. Jackson,Zendaya,Cobie Smulders");

        judul.add("Venom");
        rilis.add("3 Oktober 2018");
        genre.add("Adventure,Action");
        sinopsis.add("ini adalah sinopsis");
        durasi.add("2 Jam 20 Menit");
        cast.add("Tom Hardy,Michelle Williams,Riz Ahmed,Scott Haze,Reid Scott");

        judul.add("Thor : Ragnarok");
        rilis.add("25 Oktober 2017");
        genre.add("Adventure,Action");
        sinopsis.add("ini adalah sinopsis");
        durasi.add("2 Jam 10 Menit");
        cast.add("Chris Hemsworth,Tom Hiddleston,Cate Blanchett,Idris Elba");
        show();
        while (scan.hasNext()) {
            char select = scan.next().charAt(0);
            switch (select) {
                case 'd':
                    int number = scan.nextInt();
                    detail(number);
                    break;
                case 'r':
                    int x = scan.nextInt();
                    remove(x);
                    show();
                    break;
                case 'a':
                    System.out.print("judul\t :");
                    tambah();
                    break;
                case 's':
                    String nama = scan.next();
                    search(nama);
            }
        }
    }

    public static void detail(int i) {
        i -= 1;
        System.out.println("JUDUL    : " + judul.get(i));
        System.out.println("RILIS    : " + rilis.get(i));
        System.out.println("GENRE    : " + genre.get(i));
        System.out.println("DURASI   : " + durasi.get(i));
        System.out.println("SINOPSIS : " + sinopsis.get(i));
        System.out.println("CAST     : " + cast.get(i));
        System.out.println("d(detail)\t s(search)\t r(remove)\t a(add)\t ctrl+c(selesai)\t");
    }

    public static void remove(int i) {
        judul.remove(i - 1);
        for (int x = 0; x < judul.size(); x++)
            System.out.println(judul.get(x));
        System.out.println("d(detail)\t s(search)\t r(remove)\t a(add)\t ctrl+c(selesai)\t ");
    }

    public static void tambah() {
        Scanner scan = new Scanner(System.in);
        System.out.print(" ");
        judul.add(scan.nextLine());
        System.out.print("RILIS\t :");
        rilis.add(scan.nextLine());
        System.out.print("DURASI\t :");
        durasi.add(scan.nextLine());
        System.out.print("GENRE\t :");
        genre.add(scan.nextLine());
        System.out.print("SINOPSIS :");
        sinopsis.add(scan.nextLine());
        System.out.print("CAST\t :");
        cast.add(scan.nextLine());
        System.out.println("d(detail)\t s(search)\t r(remove)\t a(add)\t ctrl+c(selesai)\t");
    }

    public static void search(String nama) {
        for (int x = 0; x < judul.size(); x++) {
            if (judul.get(x).contains(nama)) {
                System.out.println(x + 1 + ". " + judul.get(x));
            }
        }
        System.out.println("d(detail)\t s(search)\t r(remove)\t a(add)\t ctrl+c(selesai)\t");
    }

    public static void show() {
        for (int x = 0; x < judul.size(); x++) {
            System.out.println(x + 1 + ". " + judul.get(x));
        }
        System.out.println("d(detail)\t s(search)\t r(remove)\t a(add)\t ctrl+c(selesai)\t");
    }
}