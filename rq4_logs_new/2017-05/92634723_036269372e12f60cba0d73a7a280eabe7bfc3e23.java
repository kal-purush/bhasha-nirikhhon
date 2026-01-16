/** NetId: jl3263. Time spent: 7 hours, 15 minutes.
 An instance maintains info about the PHD of a person. */

/** Checked Javadoc output and was OK.*/

public class PHD {
	private String name; // name of person with PHD, its length > 0
	private char gender; // gender of PHD person, 'f' for female, 
						/*'m' for male*/
	private int year; // year PHD awarded
	private int month; // month PHD awarded.In range 1..12 with 1 being 
						/*January, etc.*/
	private PHD adv1; // first PHD advisor of person - null if unknown
	private PHD adv2; // second PHD advisor of person - null if unknown or 
					/*only have one advisor. If first advisor null, second 
					/*advisor must be null*/
	private int numadv; // number of PHD advisees of this person

	/**GROUP A*/

	/** Constructor: an instance for a person with name n, gender g, 
	 * PHD year y, and PHD month m. Its advisors are unknown, and it has 
	 * no advisees.
	 * Precondition: n has at least 1 char. m is in 1..12. g is 'f' for 
	 * female or 'm' for male*/
	public PHD (String n, char g, int y, int m) {
		assert n != null && n.length() >= 1;
		assert (m >= 1 && m <= 12);
		assert g == 'm' || g == 'f';
		name = n;
		gender = g;
		year = y;
		month = m;
		adv1 = null;
		adv2 = null;
		numadv = 0;
	}

	/**GETTER METHODS*/

	/**Return the name of this person.*/
	public String getName(){
		return name;
	}

	/** Return the year this person got their PHD.*/
	public int getYear(){
		return year;
	}

	/**Return the month this person got their PHD.*/
	public int getMonth(){
		return month;
	}

	/** Return the value of the sentence "This person is male."*/
	public boolean isMale(){
		return gender == 'm';
	}

	/**Return the first advisor of this PHD (null if unknown).*/
	public PHD advisor1(){
		return adv1;
	}

	/**Return the second advisor of this PHD (null if unknown or 
	 * non-existent).*/
	public PHD advisor2(){
		return adv2;
	}

	/**Return the number of PHD advisees of this person.*/
	public int numAdvises(){
		return numadv;
	}

	/**GROUP B*/

	/**SETTER METHODS*/

	/**Add p as the first advisor of this person.
	 * Precondition: the first advisor is unknown and p is not null.*/
	public void setAdvisor1(PHD p){
		assert adv1 == null && p != null;
		adv1 = p;
		p.numadv = p.numadv + 1;
	}

	/**Add p as the second advisor of this person.
	 * Precondition: The first advisor (of this person) is known, 
	 * the second advisor is unknown, p is not null, and p is different 
	 * from the first advisor.*/
	public void setAdvisor2(PHD p){
		assert adv1 != null && adv2 == null && p != null && p != adv1;
		adv2 = p;
		p.numadv = p.numadv + 1;
	}

	/**GROUP C*/

	/**Constructor: a PHD with name n, gender g, PHD year y, PHD month m,
	 * first advisor adv, and no second advisor.
	 * Precondition: n has at least 1 char, g is 'f' for female or 'm' 
	 * for male, m is in 1..12, and adv is not null.*/
	public PHD(String n, char g, int y, int m, PHD adv){
		assert n != null && n.length() >= 1; 
		assert g == 'f'|| g == 'm';
		assert m >= 1 && m <= 12;
		assert adv != null;
		name = n;
		gender = g;
		year = y;
		month = m;
		adv1 = adv;
		adv2 = null;
		numadv = 0;
		adv.numadv = adv.numadv + 1;
	}

	/**Constructor: a PHD with name n, gender g, PHD year y, PHD month m,
	 * first advisor adv1, and second advisor adv2.
	 * Precondition: n has at least 1 char. g is 'f' for female or 'm' 
	 * for male. m is in 1..12. adv1 and adv2 are not null. 
	 * adv1 and adv2 are different.*/
	public PHD(String n, char g, int y, int m, PHD adv1, PHD adv2){
		assert n != null && n.length() >= 1; 
		assert g == 'f'|| g == 'm';
		assert m >= 1 && m <= 12;
		assert adv1 != null && adv2 != null && adv1 != adv2;
		name = n;
		gender = g;
		year = y;
		month = m;
		this.adv1 = adv1;
		this.adv2 = adv2;
		numadv = 0;
		adv1.numadv = adv1.numadv + 1;
		adv2.numadv = adv2.numadv + 1;
	}

	/**GROUP D*/

	/**Return value of "this person got their PHD before p did."
	 * Precondition: p is not null.*/
	public boolean gotBefore(PHD p){
		assert p != null;
		return this.year < p.year || (this.year == p.year && 
				this.month < p.month);
	}

	/**Return value of “p is not null and this person and p are
	 * intellectual siblings."*/
	public boolean arePHDSiblings(PHD p){
		return p != null && p != this &&
				(this.adv1 != null && this.adv2 != null) &&
				(p.adv1 == this.adv1 || p.adv2 == this.adv2 || 
				p.adv1 == this.adv2 ||p.adv2 == this.adv1);
	}
}