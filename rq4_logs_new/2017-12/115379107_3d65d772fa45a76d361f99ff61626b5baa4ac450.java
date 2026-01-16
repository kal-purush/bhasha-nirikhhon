import java.util.TreeMap;

public abstract class Person {
	private String name;
	private double openness;
	private double conscientiousness;
	private double extraversion;
	private double agreeableness;
	private double neuroticism;
	private boolean isFemale;
	private TreeMap<Double, String> topMatches;
	
	public Person(String name, double openness, double conscientiousness, double extraversion, double agreeableness, double neuroticism) {
		this.name = name;
		this.openness = openness;
		this.conscientiousness = conscientiousness;
		this.extraversion = extraversion;
		this.agreeableness = agreeableness;
		this.neuroticism = neuroticism;
	}
	
	public String getName() {
		return name;
	}
	
	public double getOpenness() {
		return openness;
	}
	
	public double getConscientiousness() {
		return conscientiousness;
	}
	
	public double getExtraversion() {
		return extraversion;
	}
	
	public double getAgreeableness() {
		return agreeableness;
	}
	
	public double getNeuroticism() {
		return neuroticism;
	}
	
	public boolean getGender() {
		return isFemale;
	}
}