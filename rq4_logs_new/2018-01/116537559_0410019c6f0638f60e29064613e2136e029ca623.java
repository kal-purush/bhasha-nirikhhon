/**
 * Created by Jun on 1/14/2018.
 */

public class Custom_Food {
    private String name;
    private int num_calories;


    Custom_Food() {
        this.name = "";
        this.num_calories = 0;
    }

    Custom_Food(String n, int c) {
        this.name = n;
        this.num_calories = c;
    }

    public String Get_Name() {
        return this.name;
    }

    public int Get_Calories() {
        return this.num_calories;
    }

    public void Set_Name(String n) {
        this.name = n;
    }

    public void Set_Calories(int c) {
        this.num_calories = c;
    }
}