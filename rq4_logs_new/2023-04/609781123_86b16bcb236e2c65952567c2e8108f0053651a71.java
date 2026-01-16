package oop.seminar1.task1;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;

@Getter
@Setter
public class GeoTree {
    private HashMap<Node, Node> tree = new HashMap<>();
    public void append(Person p1, Person p2) {
        tree.put(new Node( p1, Relationships.PARENT, p2 ), new Node( p2, Relationships.CHILDREN, p1 ));
    }

    @Override
    public String toString() {
        return "GeoTree{" +
                "treeKey=" + tree.keySet() +
                '}';
    }
}