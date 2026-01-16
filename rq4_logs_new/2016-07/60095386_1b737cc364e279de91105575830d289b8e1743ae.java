package domain;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import resources.Activities;
import resources.Users;
import saver_loader.DataResource;


public class ActivityController extends ActivitySubject{
	ActivityController() {

	}
	
	public static void addActivity(String description, String startDate, String endDate, String label, ArrayList<String> dependencies, ArrayList<String> members) {
		try {
			DateFormat dateFormatter = new SimpleDateFormat("dd-MM-yyyy");
			Date start = dateFormatter.parse(startDate);
			Date end = dateFormatter.parse(endDate);

			// Create activity and add it to current Project
			Activities newActivity = new Activities(description, start, end, label);
			DataResource.selectedProject.addActivity(newActivity);

			// Set the dependencies in the JGraphT
			for (String element : dependencies) {

				ArrayList<Activities> activities = DataResource.selectedProject.getActivityList();

				for (Activities activity : activities) {

					if (activity.getLabel().equals(element))
						DataResource.selectedProject.addArrow(activity, newActivity);
				}
			}

			ArrayList<Users> users = DataResource.projectMembers;
			ArrayList<Users> tmp = new ArrayList<Users>();

			for (String element : members) {
				for (Users user : users) {
					if (user.getName().equals(element))
						tmp.add(user);
				}
			}
			newActivity.setMemberList(tmp);
			
			//***************************** SAVE NEW ACTIVITY TO DATABASE **********************
			DataResource.saveActivity(newActivity);
			
			notifyObservers();
		} catch (Exception exception) {
			System.out.println(exception.getMessage());
		}
	}
	
	public static void editActivity(String description, String startDate, String endDate, String label, ArrayList<String> dependencies, ArrayList<String> members) {		
		try {
			DateFormat dateFormatter = new SimpleDateFormat("dd-MM-yyyy");
			Date start = dateFormatter.parse(startDate);
			Date end = dateFormatter.parse(endDate);
			
			Activities myActivity = DataResource.selectedActivity;

			myActivity.setDescription(description);
			myActivity.setStartDate(start);
			myActivity.setEndDate(end);
			myActivity.setLabel(label);

			if (!dependencies.isEmpty()) {

				DataResource.selectedProject.resetIncomingEdges(myActivity);
				ArrayList<Activities> activities = DataResource.selectedProject.getActivityList();

				// Set the dependencies in the JGraphT
				for (String element : dependencies) {

					for (Activities activity : activities) {

						if (activity.getLabel().equals(element))
							DataResource.selectedProject.addArrow(activity, myActivity);
					}
				}
			}

			if (!members.isEmpty()) {
				DataResource.resetActivityMembers(DataResource.selectedActivity.getId());
				ArrayList<Users> users = DataResource.projectMembers;
				ArrayList<Users> tmp = new ArrayList<Users>();

				for (String element : members) {
					for (Users user : users) {
						if (user.getName().equals(element))
							tmp.add(user);
					}
				}
				DataResource.selectedActivity.setMemberList(tmp);
			}
			
			
			//******************************SAVE TO DATABASE METHOD*********************************8
			DataResource.saveActivity(DataResource.selectedActivity);
			
			notifyObservers();
		} catch (Exception exception) {
			System.out.println(exception.getMessage());
		}
	}
	
	public static void deleteActivity() {
		Activities myActivity = DataResource.selectedActivity;
		DataResource.selectedProject.deleteActivity(myActivity);
		
		notifyObservers();
	}
}