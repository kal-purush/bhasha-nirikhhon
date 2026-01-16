package problem970;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public final class PowerfulIntegers {

    public List<Integer> powerfulIntegers(int x, int y, int bound) {
        final Set<Integer> powerfulInts = new HashSet<>();

        for (int xi = 1; xi < bound; xi *= x) {
            for (int yi = 1; xi + yi <= bound; yi *= y) {
                powerfulInts.add(xi + yi);
                if (y == 1)
                    break;
            }

            if (x == 1)
                break;
        }

        return new LinkedList<>(powerfulInts);
    }

}