package task;

public class Group {
    private int idgroup;
    private int quantityhours;

    public Group(int idgroup, int quantityhours) {
        this.idgroup = idgroup;
        this.quantityhours = quantityhours;
    }

    public int getIdgroup() {
        return idgroup;
    }

    public void setIdgroup(int idgroup) {
        this.idgroup = idgroup;
    }

    public int getQuantityhours() {
        return quantityhours;
    }

    public void setQuantityhours(int quantityhours) {
        this.quantityhours = quantityhours;
    }
}