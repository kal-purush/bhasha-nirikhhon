package homework;

import homework.models.Banknote;
import homework.models.BanknotesBox;
import homework.models.BanknotesBoxImpl;
import homework.models.BankCard;
import java.util.Currency;
import java.util.Map;

public class AtmImpl implements Atm {

  private Map<Currency, BanknotesBox> banknotesBoxes;
  private BankCard currentCard;

  public AtmImpl(Map<Currency, BanknotesBox> banknotesBoxes) {
    System.out.println("Brrr... ATM was init: with banknotes");
    this.banknotesBoxes = banknotesBoxes;
  }

  @Override
  public Atm insertCard(BankCard card) {
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
  public Atm putMoney(Banknote banknote) {
    System.out.println("ATM: Add money...");
    currentCard.putMoney(Long.valueOf(banknote.getIntValue()), banknote.getCurrency());
    if (banknotesBoxes.containsKey(banknote.getCurrency())) {
      banknotesBoxes.get(banknote.getCurrency()).putMoney(banknote);
    } else {
      banknotesBoxes.put(banknote.getCurrency(), new BanknotesBoxImpl(banknote.getValue(), banknote));
    }
    return this;
  }

  @Override
  public Atm getMoney(Long amount) {
    System.out.println("ATM: Get money...");
    if (!currentCard.getAccount().hasAmount(amount)) {
      throw new RuntimeException("Not enough money on your account.");
    }
    currentCard.getMoney(amount);
    banknotesBoxes.get(currentCard.getAccount().getCurrency()).getMoney(amount);
    return this;
  }

  @Override
  public Atm displayBalance() {
    System.out.printf("Current balance is: %s for card: %s%n", currentCard.getReadableAmount(), currentCard.getCardNumber());
    return this;
  }
}