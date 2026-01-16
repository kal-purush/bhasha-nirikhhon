/*  Program Super SIMPLE (ASLINYA MLZ NGODINF)
 * 
 *  ADU WAIFU !!!!
 * 
 *  SELAMAT MENIK-MATI
 */

public class AduWaifu {
    // Attribute
    private String nama;
    private String anime;
    private int kekuatan;
    private AduWaifu lawan;

    // Constructor 1
    public AduWaifu() {
    }

    // Constructor 2
    public AduWaifu(String nama, String anime, int kekuatan, AduWaifu lawan) {
        this.nama = nama;
        this.anime = anime;
        this.kekuatan = kekuatan;
        this.lawan = lawan;
    }

    // Behaviors / Method
    public void adu() {
        if (this.lawan != null) {
            if (this.kekuatan > this.lawan.getKekuatan()) {
                System.out.println(this.nama + " [" + this.anime + "] menang melawan " + this.lawan.getNama() + " [" + this.lawan.getAnime() + "] ! Waifumu gg mz.");
            } else if (this.kekuatan < this.lawan.getKekuatan()) {
                System.out.println(this.nama + " [" + this.anime + "] kalah melawan " + this.lawan.getNama() + " [" + this.lawan.getAnime() + "] ! Waifumu lembek amat.");
            } else {
                System.out.println("Pertarungan antara " + this.nama + " [" + this.anime + "] dan " + this.lawan.getNama() + " [" + this.lawan.getAnime() + "] berakhir seri!");
            }
        } else {
            System.out.println("Tidak ada lawan yang ditentukan.");
        }
    }

    public void displayInfo() {
        System.out.println("Nama: " + nama);
        System.out.println("Anime: " + anime);
        System.out.println("Kekuatan: " + kekuatan);
    }

    // Getter dan setter
    public int getKekuatan() {
        return kekuatan;
    }

    public String getNama() {
        return nama;
    }

    public String getAnime() {
        return anime;
    }

    public void setLawan(AduWaifu lawan) {
        this.lawan = lawan;
    }

    public static void main(String[] args) {
        // Objek waifu pertama
        AduWaifu waifu1 = new AduWaifu();
        waifu1.nama = "Asuna";
        waifu1.anime = "Sword Art Online";
        waifu1.kekuatan = 90;
        
        // // Objek waifu kedua
        AduWaifu waifu2 = new AduWaifu("Rem", "Re:Zero", 100, waifu1);

        //Informasi waifu pertama
        System.out.println("Informasi waifu pertama:");
        waifu1.displayInfo();
        System.out.println();

        // Informasi waifu kedua
        System.out.println("Informasi waifu kedua:");
        waifu2.displayInfo();
        System.out.println();

        // Mengatur lawan untuk waifu
        waifu1.setLawan(waifu2);
        // waifu2.setLawan(waifu1);

        // Pertarungan antara waifu pertama dan waifu kedua
        System.out.println("Pertarungan antara waifu pertama dan waifu kedua:");
        waifu1.adu();
    }
}