package ConstructorPerson;

import java.util.Date;
import java.lang.String;

public class Main {
    public static void main(String[] args) {

        Person person1 = new Person("Ivan", 25, 180, 75, 3000);
        Person person2 = new Person("Nikolay", 15, 160, 50, 2500);
        Person person3 = new Person("Irina", 75, 170, 70, 6000);

        Pensioner pensioner = new Pensioner("Irina", 75, 170, 70, 6000, 1300); // наследник класса Персон
        Worked worked = new Worked("Irina", 75, 170, 70, 6000, 1000, 1500);//

        worked.die();// вызываем метод класса Персон
        pensioner.die();


        person3.setMoney(10000);
        person2.setMoney(5000);
        person1.printInfo();
        person1.goToWork();

        person2.printInfo();
        person2.goToWork();

        person3.printInfo();
        person3.goToWork();


        PensionFund pensionFund = new PensionFund("Irina", 75, 170, 70, 6000, "Фонд 1", true, new Date(15-10-2022), 500);



        pensionFund.setFundName("Фонд 1");
        pensionFund.setGovernmental(true);
        pensionFund.setCreationDate(new Date(15-10-2022));
        // Нельзя изменить через сеттер
        pensionFund.setParticipantCount(500);

        pensionFund.info();
    }

}


