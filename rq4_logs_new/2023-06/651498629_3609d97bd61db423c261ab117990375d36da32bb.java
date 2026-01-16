public class Griffindor extends hogwarts{
    private int nobility;
    private int honor;
    private int bravery;

    public Griffindor(String name, int magic, int transgession, int nobility, int honor, int bravery) {
        super(name,magic,transgession);
        this.nobility = nobility;
        this.honor = honor;
        this.bravery = bravery;
    }

    public int getNobility() {
        return nobility;
    }

    public int getHonor() {
        return honor;
    }

    public int getBravery() {
        return bravery;
    }
}