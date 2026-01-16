package Lessons5;

import java.util.Arrays;

public class Homework5 {
    public static void main(String[] args) {
        Employer person1 = new Employer("Иван", "Иванович", "Масков", "ГлавТест", "megatest@rickimorty.com", "9-900-700-700", 500000, 20);
        //person1.printinfo();
        Employer[] arrayEmployer = new Employer[5];
        arrayEmployer[0] = person1;
        arrayEmployer[1] = new Employer("Сэргей", "Александрович", "Тестов", "Врач", "megatest@rickimorty.com", "9-900-700-700", 500000, 2);
        arrayEmployer[2] = new Employer("ДимаБас", "Игоревич", "Петров", "Неустановлено", "megatest@rickimorty.com", "9-900-700-700", 500000, 34);
        arrayEmployer[3] = new Employer("АлексеЗас", "Григорьевич", "Сидоров", "ГлавМент", "megatest@rickimorty.com", "9-900-700-700", 1500000, 98);
        arrayEmployer[4] = new Employer("ДеноМор", "Гадич", "Джобсов", "ГлавВрач", "megatest@rickimorty.com", "9-900-700-700", 500000, 42);
        for (int i = 0; i < arrayEmployer.length; ++i) {
            if (arrayEmployer[i].getAge() > 40) {
                arrayEmployer[i].printinfo();
            }
        }
    }
}

class Employer {
    String name;
    String middleName;
    String secondName;
    String position;
    String mail;
    String phoneNumber;
    float salary;
    int age;

    Employer(String n, String mN, String sN, String p, String m, String pN, float s, int a) {
        name = n;
        middleName = mN;
        secondName = sN;
        position = p;
        mail = m;
        phoneNumber = pN;
        salary = s;
        age = a;
    }

    void printinfo() {
        System.out.println("ФИО: " + secondName + " " + name + " " + middleName + '\n' +
                "Должность: " + position + '\n' +
                "Мэил: " + mail + '\n' +
                "Телефон: " + phoneNumber + '\n' +
                "Зарплата: " + salary + '\n' +
                "Возраст: " + age + '\n');
    }

    int getAge() {
        return age;
    }
}

