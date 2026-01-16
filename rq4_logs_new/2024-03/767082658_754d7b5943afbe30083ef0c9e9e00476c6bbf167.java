import java.util.ArrayList;

public class NameSort 
	{
		public static void compare()
		{
			ArrayList<Student> list = new ArrayList<>();
			String[] students = new String[StudentList.studentList.size()];
			for(int i=0; i<StudentList.studentList.size(); i++)
			{
		
				students[i] = StudentList.studentList.get(i).getLastName();
						
			}
			
			 for (int i = 0; i < students.length-1; i++)
		 		{
				 int minIndex = i;
				 for (int j = i + 1; j < students.length; j++)
				 	{
					 if (students[j].compareTo(students[minIndex])<0)
					 	{
				 			minIndex = j;
					 	}
				 	}
				 	String temp = students[i];
				 	students[i] = students[minIndex];
				 	students[minIndex] = temp;
				}
			 
			 
			 for(int i=0; i<StudentList.studentList.size(); i++)
			 {
				 if(!StudentList.studentList.get(i).getLastName().equals(students[i]))
				 {
					 for(int n=0; n<StudentList.studentList.size(); n++)
					 {
						 if(students[n].equals(StudentList.studentList.get(i).getLastName()))
						 {
							list.add(StudentList.studentList.get(i));
						 }
					 }
				 }
			 }
			 
			 for(int i=0; i<StudentList.studentList.size()-1; i++)
			 {
				 System.out.print(list.get(i).getFirstName()+ " ");
				 System.out.println(list.get(i).getLastName());
			 }
		}
	}