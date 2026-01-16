package movies;

public class Movie {
    /*
    TODO: - It should have private fields for name and category,
            and a constructor that sets both of these.
          - Create methods to access these properties and
            change them (getters and setters).
    */
    private String name;
    private String category;

    public Movie(String title, String category){
        this.name = title;
        this.category = category;
    }

    public void setInfo(String title, String category){
        this.name = title;
        this.category = category;
    }

    public String getName(){
        return this.name;
    }

    public String getCategory(){
        return this.category;
    }

}