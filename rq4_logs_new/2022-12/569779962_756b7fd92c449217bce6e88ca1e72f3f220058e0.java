import java.util.Iterator;
import java.util.List;

public class FamilyTreeIterator implements Iterator<Person> {
    private List<Person> familyTree;
    private int index;

    public FamilyTreeIterator(List<Person> familyTree) {
        this.familyTree = familyTree;
    }

    @Override
    public boolean hasNext() {
        return index < familyTree.size();
    }

    @Override
    public Person next() {
        return familyTree.get(index++);
    }
}