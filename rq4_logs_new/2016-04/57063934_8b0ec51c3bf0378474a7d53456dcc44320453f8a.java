package edu.saintjoe.cs.wills.RealWorldWKS;

public class StudentDriver {

	public static void main(String[] args) {
		
    int studentCount = 0;
		
	Section cmp112 = new Section("CMP112A", "B. Capouch", "Core 125");
		
		Student nextStudent = new Student("Jared Alt", "M", 1, 19);
		cmp112.students[studentCount++] = nextStudent;
		nextStudent = new Student("Jude Brace", "M", 2, 20);
		cmp112.students[studentCount++] = nextStudent;
		nextStudent = new Student("Zach Bobos", "M", 1, 19);
		cmp112.students[studentCount++] = nextStudent;
		System.out.println(cmp112.toString());
		

	}

		
		
		
	}

