package com.patterns.factory;

import com.patterns.factory.abstr.army.Commander;
import com.patterns.factory.abstr.army.General;
import com.patterns.factory.abstr.army.Private;
import com.patterns.factory.abstr.factory.ArmyFactory;
import com.patterns.factory.factory.ElfArmyFactory;
import com.patterns.factory.factory.OrcArmyFactory;

public class Main {

    /**
     * входная точка программы, демоснттрирует работу 2-х реализаций ArmyFactory
     * @param args
     */
    public static void main(String[] args) {
        createArmyByFactory(getElfFactory());
        System.out.println();
        createArmyByFactory(getOrcFactory());
    }

    /**
     * Метод демонстрирует работу фабрики. Создает все боевые единицы и вызывает их действующие методы
     * @param armyFactory - фабрика переданная для демонстрации
     */
    private static void createArmyByFactory(ArmyFactory armyFactory) {
        Commander commander = armyFactory.makeCommander();
        General general = armyFactory.makeGeneral();
        Private privatE = armyFactory.makePrivate();

        commander.makeAnOrder();
        general.command();
        privatE.executeAnOrder();
    }

    /**
     *
     * @return реализация орочьей фабрики
     */
    private static ArmyFactory getOrcFactory() {
        return new OrcArmyFactory();
    }

    /**
     *
     * @return реализация эльфской фабрики
     */
    private static ArmyFactory getElfFactory() {
        return new ElfArmyFactory();
    }
}