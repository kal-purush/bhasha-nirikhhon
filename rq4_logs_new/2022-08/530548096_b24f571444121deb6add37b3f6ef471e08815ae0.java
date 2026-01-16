package lector;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Area {
    private int id;
    private String name;

    public String toSQL() {
        return "INSERT IGNORE INTO lkup_area(id, name) VALUES('" + id + "', '" + name + "');";
    }

}