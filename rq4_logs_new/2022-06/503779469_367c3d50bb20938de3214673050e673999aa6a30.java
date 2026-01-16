import java.util.Random;
public class Gryffindor extends Hogwarts{
    private int nobility;   //Р±Р»Р°РіРѕСЂРѕРґСЃС‚РІРѕ
    private int honour;      //С‡РµСЃС‚СЊ
    private int bravery;    //С…СЂР°Р±СЂРѕСЃС‚СЊ
    protected Gryffindor(String name, String surname) {
        super(name, surname);
        nobility = new Random().nextInt(100);
        honour = new Random().nextInt(100);
        bravery = new Random().nextInt(100);
    }

    public int getBravery() {
        return bravery;
    }

    public int getHonour() {
        return honour;
    }

    public int getNobility() {
        return nobility;
    }

    public void setBravery(int bravery) {
        this.bravery = bravery;
    }

    public void setHonour(int honor) {
        this.honour = honour;
    }

    public void setNobility(int nobility) {
        this.nobility = nobility;
    }
    public int getStudentScore(){
        return honour + nobility + bravery;
    }
}