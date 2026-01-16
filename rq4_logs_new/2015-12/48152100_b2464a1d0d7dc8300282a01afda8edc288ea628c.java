package data;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import util.Communicator;

public class User {
	private String name;
	private double points;
	Communicator communicator = Communicator.getInstance();
	DecimalFormat df = new DecimalFormat("#.##");
	
	private String fileName;
	
	public User(String name) {
		super();
		this.name = name;
		fileName = name+"-user-points.txt";
		
		try {
			try (FileReader reader = new FileReader(fileName)) {
				Scanner scanner = new Scanner(reader);
				this.points = scanner.nextDouble();
			}
		} catch (FileNotFoundException e) {
			try {	
				try (FileWriter writer = new FileWriter(fileName)) {
					this.points = 0;
				}
			} catch (IOException ex) {
				Logger.getLogger(User.class.getName()).log(Level.SEVERE, null, ex);
			}
		} catch (IOException ex) {
			Logger.getLogger(User.class.getName()).log(Level.SEVERE, null, ex);
		}
		
		
	}
	
	public void adjustPoints(double points)
	{
		this.points += points;
		communicator.displayMessage("Current Points of " + name + " are " + df.format(this.points));
		try {
			store();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public double getPoints()
	{
		return points;
	}
	
	public String getName()
	{
		return name;
	}
	
	public void store() throws IOException
	{
		try (FileWriter writer = new FileWriter(fileName)) {
			writer.write(""+points);
		}
	}
}