package adventofcode.day6;

public class Border extends GridElement {
    private static ElementType elementType = ElementType.BORDER;

    public Border(int x, int y) {
        super(elementType, x, y);
    }
}