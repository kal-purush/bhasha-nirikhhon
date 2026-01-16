
public class InputInform {
	
	public static void main(String[] args) {
		String name;
		String surName;
		String secondName;
		String nameOfGroup;
		String mainActivity;
		String hobbies;
		
		name="�����";
		surName="�������";
		secondName="�������������";
		nameOfGroup="1706v";
		mainActivity="���������";
		hobbies="����������������, ���������";
		System.out.println("information about me in a tabular form: ");
		
		System.out.println("-------------------------------------------------------------------------------------------------------------------------------------");
		System.out.print("|   " + "Name" + "     |" + "    " + "Surname" + "     "+ "|     " + "SecondName" + "     |"+"    " + "Name of group" + "     ");
		System.out.println("|   " + "Main activity" + "    |" + "              " + "Hobbies" + "               |");
		System.out.println("-------------------------------------------------------------------------------------------------------------------------------------");
		//System.out.println("________________________________________________________________________________________________________________________");
		System.out.print("|   " + name + "    |" + "    " + surName + "     "+ "|   " + secondName + "    |"+"       " + nameOfGroup + "          ");
		System.out.println("|   " + mainActivity + "        |" + "    " + hobbies + "     |");
		System.out.println("-------------------------------------------------------------------------------------------------------------------------------------");
	}

	 
}