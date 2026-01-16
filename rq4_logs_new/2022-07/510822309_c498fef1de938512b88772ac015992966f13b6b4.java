package ru.sber.exceptions;

import java.io.*;

public class BankOnline {

    public static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch(NumberFormatException e){
            return false;
        }
    }

    private boolean checkBlockedCard(String cardNumber) {
        File f = new File("Exceptions/src/ru/sber/exceptions/BlockedCards.txt");
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(f))) {
            String blockedCard = bufferedReader.readLine();
            while (blockedCard != null){
                if (cardNumber.equals(blockedCard))
                    return true;
                blockedCard = bufferedReader.readLine();
            }
        } catch (FileNotFoundException exceptionFileNotFound) {
            System.out.println("Не удается найти файл по указанному пути");
            exceptionFileNotFound.printStackTrace();
        } catch (IOException exception) {
            System.out.println("Произошла ошибка");
            exception.printStackTrace();
        }
        return false;
    }

    public void send(String cardNumber, Double money) throws InvalidCardNumberException, NullParameterException, BlockedCardException, NegativeTransferAmountException, OutOfLimitTransferException {
        if (cardNumber == null || money == null) {
            throw new NullParameterException("Не все данные для перевода введены");
        }

        String cardNumberNoSpace = cardNumber.replace(" ", "");

        if (isNumeric(cardNumberNoSpace) == false || cardNumberNoSpace.length() != 16) {
            throw new InvalidCardNumberException("Неправильно указан номер карты");
        }
        if (checkBlockedCard(cardNumber)) {
            throw new BlockedCardException("Карта заблокирована");
        }
        if (money < 0) {
            throw new NegativeTransferAmountException("Неправильно указана сумму для перевода");
        }
        if (money > 50000) {
            throw new OutOfLimitTransferException("Сумма перевода не может превышать 50000");
        }
        System.out.println(String.format("Перевод на карту с номером %s успешно проведен! Сумма перевода: %s", cardNumber, money));
    }
}