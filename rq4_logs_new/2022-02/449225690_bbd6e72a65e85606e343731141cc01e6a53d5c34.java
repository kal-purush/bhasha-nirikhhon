package menuManegment.demo.menu.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MenuStructure implements Serializable{

    private String id;
    private String bind;
    private List<MenuStructure> children;

    public int size(){
        if(children == null || children.isEmpty()){
            return 1;
        }
        return 1 + children.stream().mapToInt(MenuStructure::size).sum();
    }
    public static int sizeOfList(List<MenuStructure> json){
        if(json == null || json.isEmpty()){
            return 0;
        }
        return json.stream().mapToInt(MenuStructure::size).sum();
    }

    private Stream<String> flatten(){
        if(children == null || children.isEmpty()){
            return Stream.of(id);
        }
        return Stream.concat(Stream.of(id), children.stream().flatMap(MenuStructure::flatten));

    }
    public Set<String> flattenList(){
        return flatten().collect(Collectors.toSet());
    }

    public static Set<String> flattenList(List<MenuStructure> json){
        return json.stream().flatMap(MenuStructure::flatten).collect(Collectors.toSet());
    }

}