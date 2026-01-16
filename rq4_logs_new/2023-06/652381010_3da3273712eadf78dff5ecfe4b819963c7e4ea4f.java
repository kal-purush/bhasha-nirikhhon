
import java.util.ArrayList;

/**
 * プレイヤーはモンスターデッキを持つ
 *
 * Deck：MonsterのArrayList
 */
public class Player {
  ArrayList<Monster> deck = new ArrayList<>();

  public void drawMonsters() {
    for (int i = 0; i < 5; i++) {
      this.deck.add(new Monster());
    }
  }

  public void showDeck() {
    for (Monster m : this.deck) {
      System.out.println(m);
    }
  }

}