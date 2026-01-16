package productionline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiDriver {

  public static void getMultiFunction(){

    //step 13-16
    ArrayList<Product> sortedList = new ArrayList<Product>();

    sortedList.add(new AudioPlayer("Boombox", "Sorcery"));
    sortedList.add(new MoviePlayer("55 inch TV",
        new Screen("3440x2000", 100, 20), MonitorType.LED));

    sortedList.add(new AudioPlayer("Flux Capacitor", "Time Travel"));
    sortedList.add(new MoviePlayer("Bigger than Shaq TV",
        new Screen("3976x2200", 60, 10), MonitorType.LCD));

    sortedList.add(new AudioPlayer("Loud Noise Do Hickey",
        "Evil Young People"));
    sortedList.add(new MoviePlayer("Russian TV",
        new Screen("500x250", 15, 500), MonitorType.LCD));

    Collections.sort(sortedList);
    printList(sortedList);
  }

  private static void printList(List<Product> list)
  {
    for(Product p : list)
      System.out.println(p +"\n");
  }
}