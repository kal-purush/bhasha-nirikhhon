package Lesson1and2;

public class Main {
	public static void main(String[] args) {
		Humans.createHuman(new Humans("Vasya","Petrov","Kirillovich",Human.Gender.Male),
				Humans.allHumans);
		Humans.createHuman(new Humans("Petr","Vasylev","Anatolyevich",Human.Gender.Male),
				Humans.allHumans);
		Humans.createHuman(new Humans("Arina","Rodionova","Fedorovna", Human.Gender.Female),
				Humans.allHumans);
		Humans.createHuman(new Humans("Marina","Sidorova","Fedorovna", Human.Gender.Female),
				Humans.allHumans);

		Humans.allHumans.get(0).DoChild(Humans.allHumans.get(2), Humans.genealogicTree);
		Humans.PrintTreeByID();
	}
}