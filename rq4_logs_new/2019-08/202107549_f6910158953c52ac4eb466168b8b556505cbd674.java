
public class RegisttonRecord {

	private Student student;
	private Subject [] subjects;
	int count;
	public RegisttonRecord() {
		count = 0;
		student = new Student();
		subjects = new Subject [10];
		for(int i =0;i<10;i++)
			subjects[i] = new Subject();
	}
	
	public RegisttonRecord(int n) {
		count = 0;
		student = new Student();
		subjects = new Subject [n];
		for(int i =0;i<n;i++)
			subjects[i] = new Subject();
	}
	
	public void addSubject(Subject s) {
		if(count < subjects.length-1) {
			subjects[count] = s;
			count++;
		}
	}
	
	public void display() {
		for(int i =0 ;i< count;i++){
			System.out.println(subjects);
		}
	}
}