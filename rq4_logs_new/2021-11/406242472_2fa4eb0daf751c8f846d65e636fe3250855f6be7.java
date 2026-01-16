import java.util.Scanner;

public class Main {

  public static void main(String[] args) {
    var list = new MyLinkedList();
    var sorter = new Sorter(list);
    sorter.start();

    var scanner = new Scanner(System.in);
    while (true) {
      String str = scanner.nextLine();
      if (str.equals(".")) {
        sorter.interrupt();
        break;
      } else if (str.isEmpty()) {
        if (list.getSize() > 0) {
          synchronized (list.getHead()) {
            var curr = list.getHead().getNext();
            for (var i = 0; i < list.getSize(); i++) {
              System.out.println(curr.getMsg());
              curr = curr.getNext();
            }
          }
        } else {
          System.out.println("list is empty");
        }
      } else {
        list.addFirst(str);
      }
    }
  }
}