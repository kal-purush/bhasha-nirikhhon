import java.util.ArrayList;
import java.util.Scanner;

class Info {
    int episode = 0;
    int season = 0;
    String status;

    public Info() { // Constructor
    }

    public Info(int episode, String status) {
        this.episode = episode;
        this.status = status;
    }

    public int getEpisode() {
        return episode;
    }

    public void setEpisode(int episode) {
        this.episode = episode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }
}

public class Anime {
    String judul; // yang kayak begini itu atribut
    Info info = new Info();
    String genre;
    String studio;

    public Anime(String judul, int infoSeason, int infoEpisode, String infoStatus, String genre, String studio) {
        this.judul = judul;
        this.info.season = infoSeason;
        this.info.episode = infoEpisode;
        this.info.status = infoStatus;
        this.genre = genre;
        this.studio = studio;
    }

    public Anime(String judul, int infoSeason, int infoEpisode, String genre, String studio) {
        this.judul = judul;
        this.info.season = infoSeason;
        this.info.episode = infoEpisode;
        this.info.status = "on going";
        this.genre = genre;
        this.studio = studio;
    }

    public void displayInfo() { // ini behavior, ini metod baru
        System.out.println("===============Info Anime===============");
        System.out.println("Judul           : " + this.getJudul());
        System.out.println("Season          : " + this.getInfo().getSeason());
        System.out.println("Episode         : " + this.getInfo().getEpisode());
        System.out.println("Status          : " + this.getInfo().getStatus());
        System.out.println("Genre           : " + this.getGenre());
        System.out.println("Studio          : " + this.getStudio());
    }

    public static void tampilkanInfoAnime() {
        System.out.println("===============Info Seluruh Anime===============");
        for (Anime anime : animeList) {
            anime.displayInfo();
            System.out.println("=============================================");
        }
    }

    public static void cariAnime() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Masukkan judul atau genre anime : ");
        String searchQuery = scanner.nextLine();

        System.out.println("===============Hasil Pencarian===============");
        for (Anime anime : animeList) {
            if (anime.getJudul().toLowerCase().contains(searchQuery.toLowerCase()) ||
                    anime.getGenre().toLowerCase().contains(searchQuery.toLowerCase())) {
                anime.displayInfo();
                System.out.println("=============================================");
            }else {
                System.out.println("Tidak ada yang sesuai");
                break;
            }
        }
        scanner.close();
    }

    public String getJudul() {
        return judul;
    }

    public void setJudul(String judul) {
        this.judul = judul;
    }

    public Info getInfo() {
        return info;
    }

    public void setInfo(Info info) {
        this.info = info;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getStudio() {
        return studio;
    }

    public void setStudio(String studio) {
        this.studio = studio;
    }

    static ArrayList<Anime> animeList = new ArrayList<>();

    public static void tambahAnime() {
        animeList.add(new Anime("Tensura", 3, 2, "Fantasi", "Eight Bit"));
        animeList.add(new Anime("Demond Slayer", 4, 55, "Hiatus", "Action", "Ufotable"));
        animeList.add(new Anime("Attack On Titan", 4, 87, "Tamat", "Action", "Wit Studio and Mappa"));
    }

    public static void main(String[] args) { // ini namanya main metod
        Scanner scanner = new Scanner(System.in);

        tambahAnime();

        System.out.println("Pilih aksi:");
        System.out.println("1. Tampilkan Info Seluruh Anime");
        System.out.println("2. Cari Anime Berdasarkan Judul atau Genre");

        int choice = scanner.nextInt();
        scanner.nextLine(); // Consume newline

        switch (choice) {
            case 1:
                tampilkanInfoAnime();
                break;
            case 2:
                cariAnime();
                break;
            default:
                System.out.println("Pilihan tidak valid");
        }

        scanner.close();
    }
}