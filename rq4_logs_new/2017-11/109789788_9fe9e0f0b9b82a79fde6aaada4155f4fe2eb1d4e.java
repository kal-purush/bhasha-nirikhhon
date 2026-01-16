public class Elevator {
    private boolean arah; //arah == true kalau naik, false kalau turun.
    private int lantaiSekarang;

    public void bergerak(boolean arah){
        System.out.println("Elevator bergerak ke lantai..");
        if(arah == true){
            this.lantaiSekarang = lantaiSekarang++;
        } else {
            this.lantaiSekarang = lantaiSekarang--;
        }
    }

    public void setArah(boolean arah) {
        this.arah = arah;
    }

    public int getLantaiSekarang() {
        return lantaiSekarang;
    }

    public void setLantaiSekarang(int lantaiSekarang) {
        this.lantaiSekarang = lantaiSekarang;
    }
    
    
}