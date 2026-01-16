
public class task {
	private String[] savedTaskNames;
	private String[] savedPriorityValues;
	
	public task(String taskName, String taskPriority) {
		saveValues(taskName,taskPriority);
		System.out.println("I got here");

	}

	private void saveValues(String taskNameSave, String taskPrioritySave) {
		for(int i = 0; i < 50; i++) {
			savedTaskNames[i] = taskNameSave;
			savedPriorityValues[i] = taskPrioritySave;
			System.out.println(savedTaskNames[i] + savedPriorityValues[i]+ "got here");
			
		}
		
	}
}