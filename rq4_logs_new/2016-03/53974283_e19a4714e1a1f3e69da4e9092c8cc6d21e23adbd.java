package mainserverjt.piratemod.crew;

import java.util.UUID;

import mainserverjt.piratemod.Main;

public class Pirate{
	
	private Main main;
	private Crew crew;
	private int rank;
	private final UUID uniekID;
	private double funding;
	private String type; //wat die doet
	//private Gear gear;
	private int level;
	
	/**
	 * maakt een nieuwe piraat aan
	 * @param main main variable
	 * @param crew is de crew dat die tot behoort
	 * @param unikId is de player zijn unik id
	 */
	public Pirate(Main main, Crew crew, UUID unikId){
		this.main = main;
		this.crew = crew;
		this.uniekID = unikId;
	}

	/**
	 * geeft de rank van de piraat
	 * @return rank als int
	 */
	public int getRank() {
		return rank;
	}

	/**
	 * set de rank van de piraat
	 * @param rank als int
	 */
	public void setRank(int rank) {
		this.rank = rank;
	}

	/**
	 * return hoe veel geld dat de persoon heeft 
	 * @return bedrag als double
	 */
	public double getFunding() {
		return funding;
	}

	/**
	 * set het bedrag dat deze persoon heeft
	 * @param funding bedrag als double
	 */
	public void setFunding(double funding) {
		this.funding = funding;
	}

	/**
	 * returnt het type van piraat deze persoon is
	 * @return type als String
	 */
	public String getType() {
		return type;
	}

	/**
	 * set het type van piraat dat dez persoon is
	 * @param type als String
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * returnt de piraat zijn huidig level
	 * @return als int 
	 */
	public int getLevel() {
		return level;
	}

	/**
	 * set het momentele level dat deze piraat is
	 * @param level als int
	 */
	public void setLevel(int level) {
		this.level = level;
	}

	/**
	 * return in welke crew deze piraat zit
	 * @return als Crew
	 */
	public final Crew getCrew() {
		return crew;
	}

	/**
	 * returnt wat het uniek id id van deze piraat is
	 * @return als UUID java.util
	 */
	public final UUID getUniekID() {
		return uniekID;
	}
	
	
}