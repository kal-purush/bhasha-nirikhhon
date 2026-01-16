package com.workout.diary.main;

import java.sql.Timestamp;
import java.util.Scanner;



import com.workout.diary.main.Exercises.Exercise;

public class DiaryApp {
	private MySQLAccess sql;
	DiaryApp(MySQLAccess sql){
		this.sql=sql;
	}
	
	/**
	 * Start of the application
	 */
	public void mainMenu(){
		boolean exit=false;
		while(!exit){
			System.out.print("Please select what you want to do\n0: Exit\n1:"
					+" Add workout\n2: Find the workout with the highest total load for "
					+"a given exercise this week\n3: Find out how many workouts you did"
					+"for an arbitrary number of days and how many hours they took in total\n");
			Scanner s = new Scanner(System.in);
			int choice=ScannerSingleton.getScanner().nextInt();
			exit=menuChoice(choice);
		}
		System.out.println("Exiting....");

	}
	
	/**Pick menu item based on the supplied user choice
	 * @param choice
	 * @return
	 */
	private boolean menuChoice(int choice){
		switch(choice){
		case 0:
			return true;
		case 1:
			addWorkout();
			return false;
		case 2:
			findWorkoutWithHighestTotalLoadForExerciseThisWeek();
			return false;
		case 3:
			findNumberOfWorkoutsInPeriodAndTotalDuration();
			return false;
		}
		return true;
			
	}
	/**Main menu item*/
	private void findNumberOfWorkoutsInPeriodAndTotalDuration() {
		System.out.println("Find number of workouts and time spent for how many days back?");
		int days=getIntFromUser();
		Pair<Integer, Double> answer=sql.getNumberOfWorkoutsAndDurationForPeriod(days);
		if(answer==null){
			System.out.println("No workouts found for last 30 days");
			return;
		}
		System.out.println(new StringBuilder().append("You performed ").append(answer.getLeft())
				.append(" workouts last ").append(days).append(" days, with a total duration of ")
				.append(answer.getRight()).append(" minutes"));		
		
	}
	/**Main menu item*/
	private void findWorkoutWithHighestTotalLoadForExerciseThisWeek() {
		System.out.println("Select the exercise you want to look at results for");
		Exercises exercises=new Exercises(sql);
		showExerciseMenu(exercises);
		String excName=getExerciseFromuser(exercises);
		System.out.println("Here are your best workouts in terms of total load pushed per exercise per workout in descending order: ");
		sql.getTonnageForLastDays(7, excName);
	}
	/**Returns a timestamp created from correct user input. If the user inputs an incorrect string, it returns null
	 * @return
	 */
	private Timestamp getTimeStampFromUser(){
		String dateString=null;
		Scanner scanner=ScannerSingleton.getScanner();
		scanner.nextLine();
		try{
		System.out.println("Please write the date and time of your workout in the format yyyy-MM-dd HH:mm:ss");
		//SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateString=scanner.nextLine();
		System.out.println(dateString);
		return Timestamp.valueOf(dateString);
		}catch(IllegalArgumentException e){
			e.printStackTrace();
		}
		return null;
	}
	/**Main menu item*/
	private void addWorkout() {
		Workout w = createWorkout();
		addSets(w);
		addPostWorkoutParams(w);
		
	}

	/**Creates a workout based on user date input
	 * @return
	 */
	private Workout createWorkout() {
		Timestamp ts=null;
		do{
		ts=getTimeStampFromUser();
		}while(ts==null);
		Workout w = new Workout(ts, sql);
		return w;
	}
	
	/**After all sets are complete, this asks the user for additional information to complete the workout row
	 * @param w The current workout
	 */
	private void addPostWorkoutParams(Workout w) {
		System.out.println("How long did the workout take in minutes?");
		Double duration=getDoubleFromUser();
		w.setDuration(duration);
		System.out.println("How was your performance on a scale of 1 to 10?");
		Integer perf=getIntFromUser();
		w.setPerformance(perf);
		System.out.println("Do you have any additional notes on the workout?");
		String notes =getLineFromUser();
		w.setNotes(notes);
	}
	
	/**While the loop is running, it will keep adding sets based on user input
	 * @param w
	 */
	private void addSets(Workout w) {
		String keepAddingSets=new String();
		do{
		Exercises exercises=new Exercises(sql);
		System.out.println("What exercise do you want to do this set? ");
		showExerciseMenu(exercises);
		String excName=getExerciseFromuser(exercises);
		System.out.println("You performed a set of " +excName);
		w.addSet(excName);
		System.out.println("How much weight did you use in kg?");
		Integer load=getIntFromUser();
		System.out.println("-*-*-*-*");
		System.out.println(load);
		w.setLoad(load);
		System.out.println("How many repetitions did you perform?");
		Integer reps=getIntFromUser();
		w.setNumberOfReps(reps);
		System.out.println("Do you wish to add more sets y/n?");
		keepAddingSets=getLineFromUser();
		}while(!keepAddingSets.toLowerCase().startsWith("n"));
	}
	
	/**
	 * @return A line from user input
	 */
	private String getLineFromUser(){
		Scanner scanner=ScannerSingleton.getScanner();
		scanner.nextLine();
		while(scanner.hasNextLine()){
			return ScannerSingleton.getScanner().nextLine();
		}
		return null;
		
	}
	/**
	 * @return A double from user input
	 */
	private Double getDoubleFromUser() {
		Double param=null;
		param =ScannerSingleton.getScanner().nextDouble();
		return param;
	}
	
	/**
	 * @return An int from user input
	 */
	private Integer getIntFromUser() {
		Scanner s=new Scanner(System.in);
		int param=ScannerSingleton.getScanner().nextInt();
		return param;
	}


	/**
	 * @param exercises All registered exercises in database
	 * @return The name of the exercise the user selects
	 */
	private String getExerciseFromuser(Exercises exercises) {
		String name=null;
		do{
			int choice=getIntFromUser();
			try{
				name=exercises.getAllExercises().get(choice-1).getName();
			}catch(IndexOutOfBoundsException e){
				System.out.println("Wrong choice");
			}
		}while(name==null);
		return name;
	}
	
	/**This will show a menu for the user to pick an exercise from the ones on file
	 * @param exercises
	 */
	private void showExerciseMenu(Exercises exercises) {
		
		int x=1;
		for(Exercise e:exercises.getAllExercises()){
			System.out.print(x++);
			System.out.print(": ");
			System.out.println(e.getName());
		}
	}

}