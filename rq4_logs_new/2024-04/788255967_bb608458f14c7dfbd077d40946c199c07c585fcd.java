import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main27 {
    private static List<Rekening> rekeningList = new ArrayList<>();
    private static List<Transaksi27> transaksiList = new ArrayList<>();

    public static void main(String[] args) {
        // Input data Rekening
        inputDataRekening("16030927 3084", "Wallace", "Chase Castro", "1-458-264-3263", "ligula.Nullam@tacitisociosqu.edu");
        inputDataRekening("16100617 0573", "Darius", "Julian Daniel", "1-357-843-0547", "nec@lectusjusto.org");
        inputDataRekening("16240401 2243", "Fuller", "Finn Dunlap", "571-7062", "convallis@Vestibulumanteipsum.org");
        inputDataRekening("16270525 0112", "Malcolm", "Keane Floyd", "623-0234", "porttitor.tellus.non@Curabitur.ca");
        inputDataRekening("16971204 2416", "Geoffrey", "Stephen Pratt", "1-683-416-8323", "ut.pellentesque@luctusutpellentesque.com");
        inputDataRekening("16100727 8862", "Rudyard", "Charles Morales", "650-5379", "Proin.eget@velitegestaslacinia.ca");
        inputDataRekening("16460329 4259", "Troy", "Damon Guerra", "897-7608", "pede.Suspendisse.dui@a.ca");
        inputDataRekening("16320421 3437", "Alec", "Cooper Lee", "792-4447", "non@mus.com");
        inputDataRekening("16180729 7229", "Walter", "Seth Drake", "863-8209", "Pellentesque.ut.ipsum@neque.ca");
        inputDataRekening("16950313 6823", "Simon", "Burton Gates", "592-6919", "tellus.justo.sit@commodoauctor.net");
        inputDataRekening("16850708 3528", "Kamal", "Odysseus Salas", "1-115-339-7678", "dictum@nec.edu");
        inputDataRekening("16080205 9329", "Xenos", "Colin Curry", "1-891-703-2664", "vel@ullamcorpermagna.co.uk");
        inputDataRekening("16080628 2695", "Merritt", "Clarke Roman", "1-978-632-5110", "Aliquam.gravida@vestibulumMauris.net");
        inputDataRekening("16130909 2979", "Ryder", "Joel Cunningham", "817-1766", "interdum.Curabitur.dictum@rutrumurna.edu");
        inputDataRekening("16890212 8688", "Preston", "Brock Schroeder", "1-675-400-4501", "et.ultrices@a.co.uk");
        inputDataRekening("16141008 9963", "Alec", "Baker Barton", "527-9085", "ut@aultriciesadipiscing.ca");
        inputDataRekening("16511222 7763", "Price", "Ashton Burke", "1-564-419-4285", "Proin.velit@Duisac.net");
        inputDataRekening("16720623 0943", "Devin", "Slade Flores", "977-6690", "ac@nibhAliquamornare.com");
        inputDataRekening("16771126 7372", "Ignatius", "Lionel Kane", "353-5137", "cubilia.Curae.Phasellus@Duis.com");

        // Input data Transaksi
        inputTransaksi(898214, 494048, 3587, "2021-03-09 07:54:42");
        inputTransaksi(205420, 200162, 775880, "2021-06-25 10:23:00");
        inputTransaksi(838632, 350929, 328316, "2021-09-18 23:00:04");
        inputTransaksi(230659, 204434, 690503, "2022-02-02 19:10:34");
        inputTransaksi(770592, 334245, 444267, "2020-08-11 13:36:56");
        inputTransaksi(685302, 451002, 376442, "2020-05-23 07:34:53");
        inputTransaksi(816129, 851403, 597842, "2021-07-18 19:41:20");
        inputTransaksi(989609, 333823, 802752, "2022-02-01 01:13:11");
        inputTransaksi(297103, 396116, 779589, "2021-07-03 01:09:49");
        inputTransaksi(66190, 259150, 619774, "2021-09-09 03:57:30");
        inputTransaksi(234301, 278309, 547922, "2021-08-24 13:18:39");
        inputTransaksi(243306, 869468, 50283, "2021-03-12 03:40:16");
        inputTransaksi(371045, 991242, 602034, "2021-08-06 11:48:59");
        inputTransaksi(395170, 97058, 472273, "2021-05-02 10:53:31");
        inputTransaksi(862731, 561908, 109431, "2021-07-31 08:11:00");
        inputTransaksi(556798, 31387, 725426, "2021-03-27 06:18:20");
        inputTransaksi(873982, 896213, 846142, "2021-07-18 04:06:30");
        inputTransaksi(774247, 739406, 775848, "2020-10-24 01:39:00");
        inputTransaksi(66987, 823014, 868772, "2020-12-21 05:57:59");

        // Menampilkan menu dan memanggil fungsi sesuai pilihan pengguna
        tampilkanMenu();
    }

    // Fungsi untuk input data Rekening
    public static void inputDataRekening(String noRekening, String nama, String namaIbu, String phone, String email) {
        Rekening rekening = new Rekening(noRekening, nama, namaIbu, phone, email);
        rekeningList.add(rekening);
    }

    // Fungsi untuk input data Transaksi
    public static void inputTransaksi(double saldo, double saldoAwal, double saldoAkhir, String tanggalTransaksi) {
        // Parsing tanggalTransaksi ke dalam bentuk Date
        Date tanggal = null;
        try {
            tanggal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tanggalTransaksi);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Transaksi27 transaksi = new Transaksi27(saldo, saldoAwal, saldoAkhir, tanggalTransaksi);
        transaksiList.add(transaksi);
    }

    // Menampilkan menu
    public static void tampilkanMenu() {
        Scanner scanner = new Scanner(System.in);
        int pilihan;
        do {
            System.out.println();
            System.out.println("===========SELAMAT DATANG DI===========");
            System.out.println("\u001B[32m           Bank Surya Permata           \u001B[0m");

            System.out.println(" Menu:");
System.out.println("+----+-------------------------+");
System.out.println("| 1. | Input Data Rekening     |");
System.out.println("| 2. | Input Data Transaksi    |");
System.out.println("| 3. | Tampilkan Data Rekening |");
System.out.println("| 4. | Tampilkan Data Transaksi|");
System.out.println("| 5. | Cari Saldo di Atas 500k |");
System.out.println("| 6. | Sorting by Name         |");
System.out.println("| 7. | Keluar                  |");
System.out.println("+----+-------------------------+");
System.out.print("Pilihan Anda: ");

            pilihan = scanner.nextInt();
            System.out.println();
            switch (pilihan) {
                case 1:
                    // Input data Rekening
                    inputDataRekeningFromUser(scanner);
                    break;
                case 2:
                    // Input data Transaksi
                    inputTransaksiFromUser(scanner);
                    break;
                case 3:
                    // Menampilkan data Rekening
                    tampilkanDataRekening();
                    break;
                case 4:
                    // Menampilkan data Transaksi
                    tampilkanDataTransaksi();
                    break;
                case 5:
                    // Mencari saldo di atas 500 ribu
                    cariSaldoDiAtas500Ribuan();
                    break;
                case 6:
                    // Sorting by Name
                    urutkanDataNama();
                    break;
                case 7:
                System.out.println("\u001B[92mTerima kasih!\u001B[0m");
                System.out.println();
                    break;
                default:
                System.out.println("\u001B[31mPilihan tidak valid!\u001B[0m");
                System.out.println();
                    break;
            }
        } while (pilihan != 7);
    }

    // Fungsi untuk input data Rekening dari pengguna
    public static void inputDataRekeningFromUser(Scanner scanner) {
        System.out.println("Masukkan data Rekening:");
System.out.println("+------------------+");
scanner.nextLine();
System.out.print("| No Rekening:     ");
String noRekening = scanner.nextLine();
System.out.println("+------------------+");
System.out.print("| Nama       :      ");
String nama = scanner.next();
System.out.println("+------------------+");

System.out.print("| Nama Ibu   :        ");
String namaIbu = scanner.next();
System.out.println("+------------------+");
System.out.print("| Phone      :          ");
String phone = scanner.next();
System.out.println("+------------------+");
System.out.print("| Email      :           ");
String email = scanner.next();
System.out.println("+------------------+");

        inputDataRekening(noRekening, nama, namaIbu, phone, email);
    }

    // Fungsi untuk input data Transaksi dari pengguna
    public static void inputTransaksiFromUser(Scanner scanner) {
        System.out.println("Masukkan data Transaksi:");
System.out.println("+---------------------------------+");
System.out.print("| Saldo                                  :  ");
double saldo = scanner.nextDouble();
System.out.println("+---------------------------------+");
System.out.print("| Saldo Awal                             :  ");
double saldoAwal = scanner.nextDouble();
System.out.println("+---------------------------------+");
System.out.print("| Saldo Akhir                            :  ");
double saldoAkhir = scanner.nextDouble();
System.out.println("+---------------------------------+");
System.out.print("| Tanggal Transaksi (yyyy-MM-dd HH:mm:ss):  ");
String tanggalTransaksi = scanner.next();
System.out.println("+---------------------------------+");

        inputTransaksi(saldo, saldoAwal, saldoAkhir, tanggalTransaksi);
    }

    // Fungsi untuk menampilkan data Rekening
   public static void tampilkanDataRekening() {
    System.out.println("Data Rekening:");
    System.out.println("+----------------------+-------------------------+-------------------------+----------------+----------------------------------------------+");
    System.out.println("| No Rekening          | Nama                    | Nama Ibu                | Phone          | Email                                        |");
    System.out.println("+----------------------+-------------------------+-------------------------+----------------+----------------------------------------------+");
    for (Rekening rekening : rekeningList) {
        System.out.printf("| %-20s | %-23s | %-23s | %-14s | %-44s |\n", rekening.getNoRekening(), rekening.getNama(), rekening.getNamaIbu(), rekening.getPhone(), rekening.getEmail());
    }
    System.out.println("+---------------------+-------------------------+-------------------------+--------------+-------------------------------------------------+");
}

    // Fungsi untuk menampilkan data Transaksi
    public static void tampilkanDataTransaksi() {
    System.out.println("Data Transaksi:");
    System.out.println("+------------+------------+------------+---------------------+");
    System.out.println("| Saldo      | Saldo Awal | Saldo Akhir| Tanggal Transaksi   |");
    System.out.println("+------------+------------+------------+---------------------+");
    for (Transaksi27 transaksi : transaksiList) {
        System.out.printf("| %-10.2f | %-10.2f | %-10.2f | %-19s |\n", transaksi.getSaldo(), transaksi.getSalldoAwal(), transaksi.getSaldoAkhir(), transaksi.getTanggalTransaksi());
    }
    System.out.println("+------------+------------+------------+---------------------+");
}

    // Fungsi untuk mencari saldo di atas 500 ribu
    public static void cariSaldoDiAtas500Ribuan() {
        System.out.println("Saldo di atas 500 ribu:");
        System.out.println("+------------+------------+------------+---------------------+");
        System.out.println("| Saldo      | Saldo Awal | Saldo Akhir| Tanggal Transaksi   |");
        System.out.println("+------------+------------+------------+---------------------+");
        for (Transaksi27 transaksi : transaksiList) {
            if (transaksi.getSaldo() > 500000) {
                System.out.printf("| %-10.2f | %-10.2f | %-10.2f | %-19s |\n", transaksi.getSaldo(), transaksi.getSalldoAwal(), transaksi.getSaldoAkhir(), transaksi.getTanggalTransaksi());
            }
        }
        System.out.println("+------------+------------+------------+---------------------+");
    }
    

    // Fungsi untuk mengurutkan data Rekening berdasarkan nama
    public static void urutkanDataNama() {
        Collections.sort(rekeningList, Comparator.comparing(Rekening::getNama));
        System.out.println("Data Rekening setelah diurutkan berdasarkan nama:");
        tampilkanDataRekening();
    }
}