import java.math.BigInteger;
import java.util.*;

/**
 * Algorithmen Uebung 4.
 *
 * @author Tobias Müller              193683
 * @author Peter Tim Oliver Nauroth   198322
 * @author Lukas Reichert             199034
 */
public class WalkNodes {

  private Map<String, BigInteger[]> memory;
  private BigInteger steps;

  private enum Direction {
    RIGHT,      // →
    UP,         // ↑
    UP_LEFT,    // ↖
    DOWN_RIGHT, // ↘
    UP_RIGHT,   // ↗
    DEFAULT,    // Tu nichts (Fuer den Start)
  }

  public WalkNodes() {
    Scanner s = new Scanner(System.in);

    System.out.println("Bitte geben Sie Ihr Betrag ein: (Zum beenden \"hguone\" eingeben)");
    while (true) {
      String input;
      try {
        input = s.nextLine();

        if (input.equals("hguone")) {
          System.out.println("bye");
          return;
        }
        int n = Integer.parseInt(input);

        memory = new HashMap<>();
        this.steps = BigInteger.ZERO;

        print(n, walk(n, 0, 0, Direction.DEFAULT, BigInteger.ZERO));

//        Object[] c =  memory.keySet().toArray();
//
//        for (int i = 0; i < c.length; i++) {
//          System.out.println(c[i] + "  " + memory.get(c[i]));
//        }
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Unguelitige Eingabe!");
      }
    }
  }

  /**
   * Rekurive funktion fuer das laufen.
   *
   * @param n int
   * @param x int
   * @param y int
   * @param d Direction
   * @return BigInteger
   */
  private BigInteger[] walk(int n, int x, int y, Direction d, BigInteger steps) {
    steps = steps.add(BigInteger.ONE);

    // Ueberfluessige koennen von anfang an nicht beachtet werden.
    if (x < 0 || y < 0 || x > n || y > n || n - x - y < 0) {
      return new BigInteger[] {BigInteger.ZERO, BigInteger.ZERO};
    }
    // Ueberpruefen ob man am ziel angekommen ist.
    if (x == n && y == 0) {
      return new BigInteger[] {BigInteger.ONE, steps};
    }

    // Ueberpruefen ob der Wert schon im cache liegt. (D.P.)
    String compare = x + "|" + y + "|" + d + "|" + n + "|" + steps;
    if (memory.containsKey(compare)) {
      return memory.get(compare);
    }

    BigInteger resultPaths;
    BigInteger resultSteps;

    // Einschraenkungen beachten und rekursiv weiter laufen.
    switch (d) {
      case UP:
        BigInteger[] i = walk(n, x - 1, y + 1, Direction.DOWN_RIGHT, steps);
        BigInteger[] j = walk(n, x, y + 1, Direction.UP, steps);
        BigInteger[] k = walk(n, x + 1, y - 1, Direction.UP_LEFT, steps);

        resultPaths = i[0].add(j[0]).add(k[0]);
        resultSteps = i[1].add(j[1]).add(k[1]);
        break;
      case UP_LEFT:
        BigInteger[] iu = walk(n, x + 1, y, Direction.RIGHT, steps);
        BigInteger[] ju = walk(n, x, y + 1, Direction.UP, steps);
        BigInteger[] ku = walk(n, x + 1, y - 1, Direction.UP_LEFT, steps);
        BigInteger[] lu = walk(n, x + 1, y + 1, Direction.UP_RIGHT, steps);

        resultPaths = iu[0].add(ju[0]).add(ku[0]).add(lu[0]);
        resultSteps = iu[1].add(ju[1]).add(ku[1]).add(lu[1]);
        break;
      case RIGHT:
      case UP_RIGHT:
        BigInteger[] ir = walk(n, x + 1, y, Direction.RIGHT, steps);
        BigInteger[] jr = walk(n, x - 1, y + 1, Direction.DOWN_RIGHT, steps);
        BigInteger[] kr = walk(n, x + 1, y - 1, Direction.UP_LEFT, steps);
        BigInteger[] lr = walk(n, x + 1, y + 1, Direction.UP_RIGHT, steps);

        resultPaths = ir[0].add(jr[0]).add(kr[0]).add(lr[0]);
        resultSteps = ir[1].add(jr[1]).add(kr[1]).add(lr[1]);
        break;
      case DOWN_RIGHT:
        BigInteger[] id = walk(n, x + 1, y, Direction.RIGHT, steps);
        BigInteger[] jd = walk(n, x - 1, y + 1, Direction.DOWN_RIGHT, steps);
        BigInteger[] kd = walk(n, x, y + 1, Direction.UP, steps);
        BigInteger[] ld = walk(n, x + 1, y + 1, Direction.UP_RIGHT, steps);

        resultPaths = id[0].add(jd[0]).add(kd[0]).add(ld[0]);
        resultSteps = id[1].add(jd[1]).add(kd[1]).add(ld[1]);
        break;
      default:
        BigInteger[] ide = walk(n, x + 1, y, Direction.RIGHT, steps);
        BigInteger[] jde = walk(n, x - 1, y + 1, Direction.DOWN_RIGHT, steps);
        BigInteger[] kde = walk(n, x, y + 1, Direction.UP, steps);
        BigInteger[] lde = walk(n, x + 1, y + 1, Direction.UP_RIGHT, steps);
        BigInteger[] mde = walk(n, x + 1, y - 1, Direction.UP_LEFT, steps);

        resultPaths = ide[0].add(jde[0]).add(kde[0]).add(lde[0]).add(mde[0]);
        resultSteps = ide[1].add(jde[1]).add(kde[1]).add(lde[1]).add(mde[1]);
        break;
    }

    BigInteger[] result = new BigInteger[]{resultPaths, resultSteps};
    // Ergebnis im cache speichern. (D.P.)
    memory.put(compare, result);

    return result;
  }

  /**
   * Ausdrucken in der Konsole.
   *
   * @param n     int
   * @param paths BigInteger
   */
  private void print(int n, BigInteger[] paths) {
    System.out.println("Steps: " + paths[1] + " | (" + n + ", " + paths[0] + ")");
  }

  /**
   * main Methode.
   *
   * @param args String[]
   */
  public static void main(String[] args) {
    new WalkNodes(); // Neues Objekt erzeugen.
  }
}