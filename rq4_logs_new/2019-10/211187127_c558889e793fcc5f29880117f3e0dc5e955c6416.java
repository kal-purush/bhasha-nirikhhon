import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Fragments {
    List<String> fragments = new ArrayList<>();

    public void addFragment(String item) {
        fragments.add(item);
    }

    public void addFragments(String line) {
        Collections.addAll(fragments, line.split(";"));
    }

    public Fragments getNotFoundFragments(Text text) {
        Fragments notFoundFragments = new Fragments();

        for (var fragment : fragments) {
            if (text.findFragment(fragment) == -1) {
                notFoundFragments.addFragment(fragment);
            }
        }
        return notFoundFragments;
    }

    public List<Integer> getLineNumbers(Text text) {
        List<Integer> numbers = new ArrayList<>();
        for (var fragment : fragments) {
            numbers.add(text.findFragment(fragment));
        }
        return numbers;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();

        for (var fragment : fragments) {
            sb.append(fragment.toString());
            sb.append("\n");
        }
        return sb.toString();
    }
}