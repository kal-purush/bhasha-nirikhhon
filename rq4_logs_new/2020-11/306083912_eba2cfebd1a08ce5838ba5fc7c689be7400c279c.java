public class Homework_4 {
    public static void main(String[] args) {
        Homework_4_Employee[] employees = {
                new Homework_4_Employee("Петрова Раиса Андреевна", "Начальник отдела разработки",
                        "458987", 250000, 43),
                new Homework_4_Employee("Иванов Николай Викторович", "Ведущий инженер-программист",
                        "459892", 170000, 42),
                new Homework_4_Employee("Сидоров Валерий Дмитриевич", "Инженер-программист 1й категории",
                        "454298", 135000, 32),
                new Homework_4_Employee("Антонов Евгений Александрович", "Инженер-программист",
                        "455579", 120000, 25),
                new Homework_4_Employee("Антонова Виталина Александровна", "Инженер-программист",
                        "455512", 120000, 25)
        };

        //Вывести при помощи методов из пункта 3 ФИО и должность.
        for (Homework_4_Employee emp : employees)
        System.out.println(emp.getFullName() + "; должность: " + emp.getFunction());
        System.out.println();

        System.out.println("Создать массив из 5 сотрудников. С помощью цикла вывести информацию только о сотрудниках старше 40 лет");
        for (Homework_4_Employee emp : employees) {
            if(emp.getAge() > 40)
                emp.getFullEmployeeInfo();
        }

        System.out.println("Создать метод, повышающий зарплату всем сотрудникам старше 30 лет на 5000");
        annualSalaryIncrease(employees, 30, 5000);
    }

    //* Создать метод, повышающий зарплату всем сотрудникам старше 30 лет на 5000;
    public static void annualSalaryIncrease(Homework_4_Employee[] employees, int age, int increaseAmount) {
        for (Homework_4_Employee emp : employees) {
            if (emp.getAge() >= age) {
                emp.setSalary(emp.getSalary() + increaseAmount);
                emp.getFullEmployeeInfo();
            }
        }
    }
}