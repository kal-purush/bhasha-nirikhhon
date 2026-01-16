package task_8;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.TreeSet;

/**
 * 1) Создать коллекцию номеров авто. Пример номера: Т652МХ790
 * 2) Свойства номера: серия(ТМХ), номер(652),регион(790), ФИО
 * 3) При заполнении серии дать возможность ввести серию вручную или автоматически. Номер заполняется рандомно.
 * 4) Сделать поиск владельца номера по номеру авто
 * 5) Сделать метод для проверки номера на красивый номер, например, А777АА790
 */

public class BlatniyeNomera {

    public static void main(String[] args) {
        CollectionCarNumbers carNumbers = new CollectionCarNumbers();
        carNumbers.createNumber();
        carNumbers.getAllNumbers();
        carNumbers.getDriver();

    }
}