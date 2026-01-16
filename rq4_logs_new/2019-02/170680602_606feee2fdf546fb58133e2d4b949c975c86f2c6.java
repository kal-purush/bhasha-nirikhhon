package lesson1;

import lesson1.particiant.Animal;

// 1. Разобраться с имеющимся кодом;
// 2. Добавить класс Team, который будет содержать: название команды, массив из 4-х участников
// т.е. в конструкторе можно сразу всех участников указывать), метод для вывода информации
// о членах команды прошедших дистанцию, метод вывода информации обо всех членах команды.
public class Team {
    public String name;
    public Animal[] members = new Animal[4];
    public boolean[] results = new boolean{false, false, false, false};

    // Конструктор
    public Team(String name, Animal member1, Animal member2, Animal member3, Animal member4) {
        this.name = name;
    }

    // Метод вывода информации обо всех членах команды
    public void info() {
        for (Animal animal: this.members) {
            animal.toString();
        }
    }

    // Метод для вывода информации о членах команды, прошедших дистанцию
    public void showResults() {
        for (int i = 0; i < results.length; i++) {
            if (results[i]) System.out.println("Участник "+i+" прошел полосу препятствий");
            else System.out.println("Участник "+i+" не прошел полосу препятствий");
        }
    }
}

// 3. Добавить класс Course (полоса препятствий), в котором будут находиться:
// массив препятствий, метод который будет просить команду пройти всю полосу;
class Course {
    public void doIt(Team team) {
        // прыгаем на 0,5 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].jump(0.5);
        }
        // бежим на 5 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].run(5);
        }
        // плывем на 2 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].swim(2);
        }
        // прыгаем на 1 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].jump(1);
        }
        // бежим на 15 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].run(15);
        }
        // плывем на 5 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].swim(5);
        }
        // прыгаем на 3 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].jump(3);
        }
        // бежим на 50 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].run(50);
        }
        // плывем на 15 м
        for (int i = 0; i < team.members.length; i++) {
            // если участник не прошел предыдущее испытание, к текущему он не допускается
            if (team.results[i]) team.results[i] = team.members[i].swim(15);
        }
    }
}