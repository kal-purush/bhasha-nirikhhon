package homework;

import homework.models.Banknote;
import homework.models.BanknotesBox;
import homework.models.IBankCard;
import java.util.Currency;
import java.util.HashMap;
import java.util.Map;

public class Atm implements IAtm {

  private Map<Currency, BanknotesBox> banknotesBoxes = new HashMap<>();
  private IBankCard currentCard;

  public Atm() {
    System.out.println("Brrr... ATM was init: w/o banknotes");
  }

  public Atm(Map<Currency, BanknotesBox> banknotesBoxes) {
    System.out.println("Brrr... ATM was init: with banknotes");
    this.banknotesBoxes = banknotesBoxes;
  }

  @Override
  public IAtm insertCard(IBankCard card) {
    if (currentCard != null) {
      throw new RuntimeException("Eject previous card before using.");
    }
    this.currentCard = card;
    System.out.println("ATM: Card was inserted: " + card.getCardNumber());
    return this;
  }

  @Override
  public void ejectCard() {
    this.currentCard = null;
    System.out.println("ATM: Card was ejected. Come again!");
  }

  @Override
  public IAtm putMoney(Banknote banknote) {
    System.out.println("ATM: Add money...");
    currentCard.putMoney(Long.valueOf(banknote.getValue()), banknote.getCurrency());
    if (banknotesBoxes.containsKey(banknote.getCurrency())) {
      banknotesBoxes.get(banknote.getCurrency()).putMoney(banknote);
    } else {
      banknotesBoxes.put(banknote.getCurrency(), new BanknotesBox(banknote));
    }
    return this;
  }

  @Override
  public IAtm getMoney(Long amount) {
    System.out.println("ATM: Get money...");
    if (!currentCard.getAccount().hasAmount(amount)) {
      throw new RuntimeException("Not enough money on your account.");
    }
    currentCard.getMoney(amount);
    banknotesBoxes.get(currentCard.getAccount().getCurrency()).getMoney(amount);
    return this;
  }

  @Override
  public IAtm displayBalance() {
    System.out.printf("Current balance is: %s for card: %s%n", currentCard.getReadableAmount(), currentCard.getCardNumber());
    return this;
  }
}