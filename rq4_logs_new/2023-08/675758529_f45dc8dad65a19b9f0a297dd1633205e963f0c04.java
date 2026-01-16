package lesson8.University;

import java.util.ArrayList;
import java.util.Scanner;

/**
 Делаем классы для работы с университетом.
 В универе есть массив студентов, сотрудников и общежитий.
 Студенты (поля имя, фамилия, номер группы, место проживания),
 сотрудники (поля имя, фамилия, должность - препод, декан, ректор и т д),
 общежитие (адрес, название, массив комнат). Сделать меню с 6 пунктами:
 1) Добавление студента в универ 2) Добавление сотрудника в универ
 3) Заселить студента в общежитие 4) Просмотр всех студентов
 5) Просмотр всех сотрудников 6) Выход.
 */
public class Main {
    public static void main(String[] args) {
        University university = new University();
        Boolean[] rooms = {true, true, false, false, true, false, false, false, false};


        ArrayList<Dormitory> dormitories = new ArrayList<>();
        dormitories.add(new Dormitory("ул. Пушкина, д.2", "Общежитие 10", rooms));

        university.setDormitories(dormitories);

        ArrayList<Student> students = new ArrayList<>();
        students.add(new Student("Инва", "Иванов", "25", "Общежитие 10", 0));
        students.add(new Student("Инга", "Иванович", "23", "Общежитие 10", 1));
        students.add(new Student("Юрий", "Инович", "23", "Общежитие 10", 3));
        university.setStudents(students);

        ArrayList<Staff> staff = new ArrayList<>();
        staff.add(new Staff("Директор", "Босс", "Директор"));
        university.setStaff(staff);


        boolean b = true;
        while (b) {
            System.out.println("1: Добавление студента в универ");
            System.out.println("2: Добавление сотрудника в универ");
            System.out.println("3: Заселение студента в общежитие");
            System.out.println("4: Просмотр всех студентов");
            System.out.println("5: Просмотр всех сотрудников");
            System.out.println("6: Выход");
            System.out.println("Введите цифру чтобы выполнить действие ");

            Scanner sc = new Scanner(System.in);
            int user_Input = sc.nextInt();
            switch (user_Input) {
                case 1:

                    System.out.print("Введите имя студента: ");
                    String firstName = sc.next();

                    System.out.print("Введите фамилию студента: ");
                    String lastName = sc.next();

                    System.out.print("Введите группу студента: ");
                    String num = sc.next();

                    // System.out.print("Введите общежитие: ");
                    // String dorm = sc.next();

                    //boolean c = true;
                    //  while (c) {
                    //  System.out.print("Введите комнату: ");
                    // int r = sc.nextInt();
                    //   if (rooms[r] != true) {
                    //    c = false;
                    //  rooms[r] = true;
                    //Student newStudent = new Student(firstName, lastName, num, dorm, r);
                    //university.addStudent(newStudent);
                    //}
                    //}
                    Student newStudent = new Student(firstName, lastName, num);
                    university.addStudent(newStudent);
                    break;
                case 2:
                    System.out.print("Введите имя сотрудника: ");
                    String firstNameStaff = sc.next();

                    System.out.print("Введите фамилию сотрудника: ");
                    String lastNameStaff = sc.next();

                    System.out.print("Введите должность сотрудника: ");
                    String postStaff = sc.next();
                    Staff newStaff = new Staff(firstNameStaff, lastNameStaff, postStaff);
                    university.addStaff(newStaff);

                    break;
                case 3:
                    ArrayList<Student> unassignedStudents = new ArrayList<>();
                    int count = 0;
                    for (Student student : students) {
                        String address = student.getLink_To_Dormitory();
                        if (address == null) {
                            count++;
                            unassignedStudents.add(student);
                            System.out.println(count + ": Имя: " + student.getName() + ", Фамилия: " + student.getSurname() + ", группа: " + student.getNumber_Group());
                        }
                    }
                    if (unassignedStudents.size() == 0) {
                        System.out.println("Все студенты уже заселены");
                        break;
                    } else {
                        System.out.println("Введите номер студента, которого хотите заселить:");
                        int studentIndex = sc.nextInt() - 1;
                        if (studentIndex >= 0 && studentIndex < unassignedStudents.size()) {
                            Student studentToAssign = unassignedStudents.get(studentIndex);
                            System.out.println("Введите номер общежития:");
                            String dormitoryNumber = sc.next();

                            boolean v = true;
                            while (v) {
                                System.out.println("Введите номер комнаты:");
                                int roomNumber = sc.nextInt();
                                if (rooms[roomNumber] != true) {
                                    studentToAssign.setLink_To_Dormitory(dormitoryNumber);
                                    studentToAssign.setLink_To_Room(roomNumber);
                                    System.out.println("Студент " + studentToAssign.getName() + " " + studentToAssign.getSurname() + " успешно заселен в комнату " + studentToAssign.getLink_To_Dormitory() + " общежития " + studentToAssign.getLink_To_Dormitory());
                                    v = false;
                                }
                            }
                        }
                    }
                    break;
                case 4:
                    for (Student student : students) {
                        String firstNames = student.getName();
                        String lastNames = student.getSurname();
                        String age = student.getNumber_Group();
                        String address = student.getLink_To_Dormitory();
                        int course = student.getLink_To_Room();

                        if (address == null) {
                            System.out.println("Имя: " + firstNames + ", Фамилия: " + lastNames + ", группа: " + age);
                        } else {
                            System.out.println("Имя: " + firstNames + ", Фамилия: " + lastNames + ", группа: " + age +
                                    ", Адрес: " + address + ", Комната: " + course);
                        }
                    }
                    break;
                case 5:
                    for (Staff staff1 : staff) {
                        String firstNames = staff1.getName();
                        String lastNames = staff1.getSurname();
                        String stPost = staff1.getPost();
                        System.out.println("Имя: " + firstNames + ", Фамилия: " + lastNames + ", должность: " + stPost);
                    }
                    break;
                case 6:
                    b = false;
                    break;
                default:
                    System.out.println("Вы ввели что-то не то ");
                    break;
            }
        }
    }
}