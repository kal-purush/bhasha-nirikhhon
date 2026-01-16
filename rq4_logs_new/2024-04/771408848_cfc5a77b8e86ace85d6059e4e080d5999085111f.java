package TP4_2_H071231037;

public class Series {
    String judul;
    int jumlahEpisode;
    String sutradara;
    String genre;
    String sinopsis;
   
    Series(String judul, int jumlahEpisode, String sutradara, String genre, String sinopsis) {
        this.judul = judul;
        this.jumlahEpisode = jumlahEpisode;
        this.sutradara = sutradara;
        this.genre = genre;
        this.sinopsis = sinopsis;
    }
    
    public String getJudul() {
        return judul;
    }
    public void setJudul(String judul) {
        this.judul = judul;
    }
    public int getJumlahEpisode() {
        return jumlahEpisode;
    }
    public void setJumlahEpisode(int jumlahEpisode) {
        this.jumlahEpisode = jumlahEpisode;
    }
    public String getSutradara() {
        return sutradara;
    }
    public void setSutradara(String sutradara) {
        this.sutradara = sutradara;
    }
    public String getGenre() {
        return genre;
    }
    public void setGenre(String genre) {
        this.genre = genre;
    }
    public String getSinopsis() {
        return sinopsis;
    }
    public void setSinopsis(String sinopsis) {
        this.sinopsis = sinopsis;
    }

    void displayInfo(){
        System.out.println("Judul = " + getJudul());
        System.out.println("Jumlah episode = " + getJudul());
        System.out.println("Nama sutradara = " + getSutradara());
        System.out.println("Sinopsis = " + getSinopsis());
    }

    
}