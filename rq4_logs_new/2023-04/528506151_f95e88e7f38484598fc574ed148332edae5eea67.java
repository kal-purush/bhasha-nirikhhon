public class Childrens extends Person {
    private Integer idNode;
    private Integer idMather;
    private Integer idFather;

    public Childrens(String gender, String name, String surname, String patronym, String birthday) {
        super(gender, name, surname, patronym, birthday);
    }

    public void setIdNode(Integer idNode) {
        this.idNode = idNode;
    }

    public void setIdMather(Integer idMather) {
        this.idMather = idMather;
    }

    public void setIdFather(Integer idFather) {
        this.idFather = idFather;
    }
}