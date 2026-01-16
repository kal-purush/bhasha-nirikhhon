class ListElement {

    Vector node;
    Vector parentNode;
    int value;
    double expectedValue;
    int waterCrossings;

    ListElement(Vector node, Vector parentNode, int value, double expectedValue, int waterCrossings) {
        this.node = node;
        this.parentNode = parentNode;
        this.expectedValue = expectedValue;
        this.value = value;
        this.waterCrossings = waterCrossings;
    }

}