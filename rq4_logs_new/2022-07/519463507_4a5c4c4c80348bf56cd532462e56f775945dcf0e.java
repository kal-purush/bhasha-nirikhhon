
public class Main {
    public static void  main (String [] args){

        Person[] persArray = new Person[5];
        persArray[0] = new Person("Ivanov Petr", "Engineer1", "petr@mailbox.com", "892165812", 32500, 38);
        persArray[1] = new Person("Ivanov Ivan", "Engineer2", "ivivan@mailbox.com", "892312312", 30000, 30);
        persArray[2] = new Person("Phillipov Valeriy", "Engineer3", "Phillipov@mailbox.com", "898341231", 35000, 46);
        persArray[3] = new Person("Petrov Oleg", "Engineer4", "Petrov@mailbox.com", "892378532", 36000, 29);
        persArray[4] = new Person("Pavlov Stas", "Engineer5", "Pavlov@mailbox.com", "897832312", 37000, 41);
        for (Person person: persArray){

            if (person.age > 40)
                System.out.println("Работник старше 40: " + person.name + " , возраст: " + person.age); //коммент-й

        }
    }
}