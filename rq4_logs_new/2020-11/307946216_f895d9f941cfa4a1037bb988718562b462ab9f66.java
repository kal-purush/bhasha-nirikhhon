package hw.lesson4;

public class IteratorApp {

    public static void main(String[] args) {
        showIteratorTest();
    }

    private static void showIteratorTest(){
        LinkedList<Integer> newList = new SimpleLinkedListImpl<>();
        for (int i = 5; i >= 1; i--) {
            newList.insertFirst(i);
        }
        showList(newList);

        System.out.println("* * * * * * *");

        MyIterator<Integer> newIterator = (MyIterator<Integer>) newList.iterator();

        newIterator.insertionBefore(777);
        showList(newList);
        System.out.println("* * * * * * *");

        newIterator.insertionAfter(999);
        showList(newList);
        System.out.println("* * * * * * *");

        newIterator.resetting();
        showList(newList);
        System.out.println("* * * * * * *");

        newIterator.insertionAfter(555);
        showList(newList);
        System.out.println("* * * * * * *");

        newIterator = (MyIterator<Integer>) newList.iterator();
        while (newIterator.hasNext()) {
            newIterator.remove();
            showList(newList);
            System.out.println("* * * * * * *");
        }
    }
    private static void showList(LinkedList<Integer> linkedList) {
        for (Integer item : linkedList) {
            System.out.println(item);
        }
    }
}