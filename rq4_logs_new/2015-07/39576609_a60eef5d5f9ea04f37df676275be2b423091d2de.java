import java.io.File;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.ArrayList;
public class CSVScreen 
{
	public static void main (String [] args) throws FileNotFoundException
	{
		File myFile = new File (args[0]);
		ArrayList <Database> myDatabase = new ArrayList <Database>();
		//Change file path to yours in your machine
		PrintStream out = new PrintStream (new FileOutputStream("/Users/julianljk/desktop/lsQueries/interviewFinalCSV/output.csv"));
		System.setOut(out);
		
		Scanner myScanner = null;	
		try 
		{
			myScanner = new Scanner (myFile);
		}
		catch (FileNotFoundException e)
		{
			System.err.print("FAILED.");
		}
		int counter = 0;
		while (myScanner.hasNextLine())
		{
			String currString = myScanner.nextLine();
			String [] currArray = currString.split(",");
			
//			for (String i: currArray)
//			{
//				System.out.println(i);
//			}
//			System.out.println(counter);
			counter++;
//			if (counter == 9738)
//			{
//				System.out.println("in 9738");
//			}
			arrayProcessor(currArray, myDatabase);
		}
//		System.out.println("finished processing, printing CSV");
		print(myDatabase);
	}
	public static void arrayProcessor (String [] currArray, ArrayList <Database> myDatabase)
	{
		Database currDatabase = null;
		if (myDatabase.isEmpty() || !hasDatabase(currArray[0], myDatabase))
		{
			currDatabase = new Database (currArray[0]);
			myDatabase.add(currDatabase);
		}
		else 
		{
			currDatabase = getDatabase (currArray[0], myDatabase);
			
			if (currDatabase == null)
			{
				System.out.println("itfuckedup");
			}
		}
		//check the databases for a player. 
		
		//
		Person currPerson = null;
		// 
		if (currDatabase.myPlayerList.isEmpty() || !currDatabase.hasPlayer(currArray[2]))
		{
			currPerson = new Person(Integer.parseInt(currArray[1]),currArray[2],currArray[3], countPosition(currArray[4]), currArray[5]);
			currDatabase.addPlayer(currPerson);
		}
		else
		{
			currPerson = currDatabase.getPlayer(currArray[2]);
			if (currPerson == null)
			{
				System.out.println("screwed up at person else");
			}
			currPerson.addStuff(currArray[3], countPosition(currArray[4]), currArray[5]);
		}
		
	}
	public static boolean hasDatabase (String currDb, ArrayList <Database> myDatabase)
	{
		for (int i = 0; i < myDatabase.size();i++)
		{
			if (myDatabase.get(i).name.equals(currDb))
			{
				return true;
			}
		}
		return false;
	}
	public static Database getDatabase (String currDb, ArrayList <Database> myDatabase)
	{
		for (int i = 0; i < myDatabase.size();i++)
		{
			if (myDatabase.get(i).name.equals(currDb))
			{
				return myDatabase.get(i);
			}
		}
		return null;
	}
	public static int countPosition (String currText)
	{
		switch (currText)
		{
		case "I know how to examine social problems.": 
			return 0;
		case "I know ways of addressing community problems.": 
			return 1;
		case "I know how political action by groups can solve social and environmental problems.": 
			return 2;
		case "I know how social and environmental issues are connected.":
			return 3;
		case "I know how to find out more information about social and environmental issues.": 
			return 4;
		case "I would be able to create a plan to address the issue.": 
			return 5;
		case "I would be able to get people to care about the problem.": 
			return 6;
		case "I would be able to organize and run a meeting.": 
			return 7;
		case "I would be able to make a public speech.": 
			return 8;
		case "I would be able to find and examine research related to the issue.": 
			return 9;
		case "I would be able to express my views in front of a group of people.": 
			return 10;
		default:
			return 11;
		}
	}
	public static void print (ArrayList <Database> myDatabase)
	{
		Database currDatabase;
		Person currPerson;
		String currLine = "";
		System.out.println("Db,userID,display_name,I know how to examine social problems.PRE,I know ways of addressing community problems.PRE,I know how political action by groups can solve social and environmental problems.PRE,I know how social and environmental issues are connected.PRE,I know how to find out more information about social and environmental issues.PRE,I would be able to create a plan to address the issue.PRE, I would be able to get people to care about the problem.PRE,I would be able to organize and run a meeting.PRE,I would be able to make a public speech.PRE,I would be able to find and examine research related to the issue.PRE,I would be able to express my views in front of a group of people.PRE,I know how to examine social problems.POST,I know ways of addressing community problems.POST,I know how political action by groups can solve social and environmental problems.POST,I know how social and environmental issues are connected.POST,I know how to find out more information about social and environmental issues.POST,I would be able to create a plan to address the issue.POST, I would be able to get people to care about the problem.POST,I would be able to organize and run a meeting.POST,I would be able to make a public speech.POST,I would be able to find and examine research related to the issue.POST,I would be able to express my views in front of a group of people.POST");	
		for (int i = 0; i < myDatabase.size(); i++)
		{
			currDatabase = myDatabase.get(i);
			
			for (int j = 0; j < currDatabase.myPlayerList.size(); j++)
			{
				currPerson = currDatabase.myPlayerList.get(j);
				currLine += currDatabase.name + "," + currPerson.id + "," + currPerson.name + ",";
				for (int k = 0; k < currPerson.preList.length; k++)
				{
					currLine += currPerson.preList[k] + ",";
				}
				for (int l = 0; l < currPerson.postList.length; l++)
				{
					currLine += currPerson.postList[l] + ",";
				}
				currLine = currLine.substring(0, currLine.length()-1);
				System.out.println(currLine);
				currLine = "";
			}
			
		}
	
	}
}