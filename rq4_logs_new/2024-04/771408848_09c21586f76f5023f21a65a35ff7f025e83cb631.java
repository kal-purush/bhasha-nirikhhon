package LAB_PBO_2024.Praktikum.Praktikum_05.Nomor1;
import java.util.InputMismatchException;
import java.util.Scanner;


public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int opsi;

        while (true) {
            try {
                System.out.println("===== BANGUN RUANG =====");
                System.out.println("1. KUBUS");
                System.out.println("2. BALOK");
                System.out.println("3. BOLA");
                System.out.println("4. TABUNG");

                System.out.println("\n===== BANGUN DATAR =====");
                System.out.println("5. PERSEGI");
                System.out.println("6. PERSEGI PANJANG");
                System.out.println("7. LINGKARAN");
                System.out.println("8. TRAPESIUM");
                System.out.println("\n========================");
                System.out.println("9. EXIT");

                System.out.println("\nOpsi : ");
                opsi = scanner.nextInt();

                if (opsi == 9) {
                    System.out.println("Telah keluar");
                    break;
                }

                switch (opsi) {
                    case 1:
                        System.out.print("Input Nilai sisi kubus: ");
                        double sisiKubus = scanner.nextDouble();
                        if (sisiKubus <= 0) {
                            throw new IllegalArgumentException("Sisi kubus harus lebih besar dari 0.");
                        }
                        kubus kubus = new kubus(sisiKubus);
                        System.out.println("\nLuas Permukaan Kubus: " + kubus.jumlahLuasPermukaan());
                        System.out.println("Volume Kubus: " + kubus.jumlahVolume());
                        break;

                    case 2:
                        System.out.print("Input Nilai panjang balok: ");
                        double panjangBalok = scanner.nextDouble();
                        System.out.print("Input Nilai lebar balok: ");
                        double lebarBalok = scanner.nextDouble();
                        System.out.print("Input Nilai tinggi balok: ");
                        double tinggiBalok = scanner.nextDouble();
                        if (panjangBalok <= 0 || lebarBalok <= 0 || tinggiBalok <= 0) {
                            throw new IllegalArgumentException("Panjang, lebar, dan tinggi balok harus lebih besar dari 0.");
                        }
                        balok balok = new balok(panjangBalok, lebarBalok, tinggiBalok);
                        System.out.println("\nLuas Permukaan Balok: " + balok.jumlahLuasPermukaan());
                        System.out.println("Volume Balok: " + balok.jumlahVolume());
                        break;

                    case 3:
                        System.out.print("Input Nilai jari-jari bola: ");
                        double jari2Bola = scanner.nextDouble();
                        if (jari2Bola <= 0) {
                            throw new IllegalArgumentException("Jari-jari bola harus lebih besar dari 0.");
                        }
                        bola bola = new bola(jari2Bola);
                        System.out.println("\nLuas Permukaan Bola: " + bola.jumlahLuasPermukaan());
                        System.out.println("Volume Bola: " + bola.jumlahVolume());
                        break;
                    case 4:
                        System.out.print("Input Nilai jari-jari tabung: ");
                        double jari2Tabung = scanner.nextDouble();
                        System.out.print("Input Nilai tinggi tabung: ");
                        double tinggiTabung = scanner.nextDouble();
                        if (jari2Tabung <= 0 || tinggiTabung <= 0) {
                            throw new IllegalArgumentException("Jari-jari dan tinggi tabung harus lebih besar dari 0.");
                        }
                        tabung tabung = new tabung(jari2Tabung, tinggiTabung);
                        System.out.println("\nLuas Permukaan Tabung: " + tabung.jumlahLuasPermukaan());
                        System.out.println("Volume Tabung: " + tabung.jumlahVolume());
                        break;
                    case 5:
                        System.out.print("Input Nilai sisi persegi: ");
                        double sisiPersegi = scanner.nextDouble();
                        if (sisiPersegi <= 0) {
                            throw new IllegalArgumentException("Sisi persegi harus lebih besar dari 0.");
                        }
                        persegi persegi = new persegi(sisiPersegi);
                        System.out.println("\nLuas Persegi: " + persegi.jumlahLuas());
                        System.out.println("Keliling Persegi: " + persegi.jumlahKeliling());
                        break;
                    case 6:
                        System.out.print("Input Nilai panjang persegi panjang: ");
                        double panjangPersegiPanjang = scanner.nextDouble();
                        System.out.print("Input Nilai lebar persegi panjang: ");
                        double lebarPersegiPanjang = scanner.nextDouble();
                        if (panjangPersegiPanjang <= 0 || lebarPersegiPanjang <= 0) {
                            throw new IllegalArgumentException("Panjang dan lebar persegi panjang harus lebih besar dari 0.");
                        }
                        persegiPanjang persegiPanjang = new persegiPanjang(panjangPersegiPanjang, lebarPersegiPanjang);
                        System.out.println("\nLuas Persegi Panjang: " + persegiPanjang.jumlahLuas());
                        System.out.println("Keliling Persegi Panjang: " + persegiPanjang.jumlahKeliling());
                        break;
                    case 7:
                        System.out.print("Input Nilai jari-jari lingkaran: ");
                        double jari2Lingkaran = scanner.nextDouble();
                        if (jari2Lingkaran <= 0) {
                            throw new IllegalArgumentException("Jari-jari lingkaran harus lebih besar dari 0.");
                        }
                        Lingkaran lingkaran = new Lingkaran(jari2Lingkaran);
                        System.out.println("\nLuas Lingkaran: " + lingkaran.jumlahLuas());
                        System.out.println("Keliling Lingkaran: " + lingkaran.jumlahKeliling());
                        break;
                    case 8:
                        System.out.print("Input Nilai sisi 1 trapesium: ");
                        double sisi1Trapesium = scanner.nextDouble();
                        System.out.print("Input Nilai sisi 2 trapesium: ");
                        double sisi2Trapesium = scanner.nextDouble();
                        System.out.print("Input Nilai sisi 3 trapesium: ");
                        double sisi3Trapesium = scanner.nextDouble();
                        System.out.print("Input Nilai sisi 4 trapesium: ");
                        double sisi4Trapesium = scanner.nextDouble();
                        System.out.print("Input Nilai tinggi trapesium: ");
                        double tinggiTrapesium = scanner.nextDouble();
                        if (sisi1Trapesium <= 0 || sisi2Trapesium <= 0 || sisi3Trapesium <= 0 || sisi4Trapesium <= 0 || tinggiTrapesium <= 0) {
                            throw new IllegalArgumentException("Semua sisi dan tinggi trapesium harus lebih besar dari 0.");
                        }
                        trapesium trapesium = new trapesium(sisi1Trapesium, sisi2Trapesium, sisi3Trapesium, sisi4Trapesium, tinggiTrapesium);
                        System.out.println("\nLuas Trapesium: " + trapesium.jumlahLuas());
                        System.out.println("Keliling Trapesium: " + trapesium.jumlahKeliling());
                        break;
                    default:
                        System.out.println("Pilihan tidak valid.");
                        break;
                }

                

            } catch (InputMismatchException e) {
                System.out.println("Input tidak valid. Masukkan angka.");
                scanner.nextLine();
            } catch (IllegalArgumentException e) {
                System.out.println("Error: " + e.getMessage());
            } 
        
        }scanner.close();
    }
}