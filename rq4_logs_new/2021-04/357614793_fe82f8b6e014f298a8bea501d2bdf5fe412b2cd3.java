package Lesson5;

public class Main {
    public static void main(String[] args) {
        Worker person[] = new Worker[5];

        person[0] = new Worker("Иванов","Иван", "Иванович","Генеральный директор",
                "director@mail.ru","+7999 555 2525",53,173000);
        person[1] = new Worker("Иванова","Наталья", "Михайловна","Главный бухгалтер",
                "glavbuh@mail.ru","+7999 555 3535",51,132000);
        person[2] = new Worker("Петров","Петр", "Петрович","Начальник мех.цеха",
                "petrov@outlook.com","+7923 554 8654",39,91380);
        person[3] = new Worker("Адамов","Павел", "Сергеевич","Юрист",
                "adamov@google.com","+7925 855 2138",37,78953);
        person[4] = new Worker("Большов","Николай", "Васильевич","Слесарь-ремонтник",
                "bolshov@yandex.ru","+7923 855 6314",49,41525);
        print_All_Persons(person);
    }

    private static void print_All_Persons(Worker[] person) {
        for(int personCount = 0; personCount < person.length; personCount++){
           if (person[personCount].getAge() > 40) person[personCount].printPrivateData();
        }
    }


}