public class Lop {
    public int id;
    public String name;

    public Lop() {
    }

    public Lop(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Lop{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}