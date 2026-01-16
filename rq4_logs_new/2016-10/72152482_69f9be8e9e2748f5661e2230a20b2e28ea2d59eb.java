import java.util.StringTokenizer;


public class Film {
    //Assigning variables
    private String name;
    private String code = "";
    //class constructor
    public Film(String film) {
        setName(film);
        setCode();

    }
    // setters
    public void setName(String name) {
        this.name = name;
    }
    private void setCode() {
        StringTokenizer st = new StringTokenizer(this.name, " ");
        while (st.hasMoreElements()) {
            this.code += st.nextToken().toUpperCase().charAt(0);
        }
    }
    // getters
    public String getCode() {
        return this.code;
    }
    public String getName() {
        return this.name;
    }

    public String toString() {
        return "- " + "\"" + getName() + "\"" + " Code: " + getCode();
    }


    public void print() {
        System.out.println(toString());
    }

}

