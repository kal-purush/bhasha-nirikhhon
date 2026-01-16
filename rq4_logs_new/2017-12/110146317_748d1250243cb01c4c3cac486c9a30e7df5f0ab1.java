package TextualAnalysisOfBooks;
/**
 * This class prints out the time analysis results for letter frequency.
 * @author Yihan
 *
 */
public class ECComparisonLF {

	public static void main(String[] args) {
	
		String s="alice-in-wonderland.txt";
		
		LetterFrequency lf=new LetterFrequency(s);
		LetterFrequencyB lfB=new LetterFrequencyB(s);
		char[] cArr=lf.topFrequentLetter();
		char[] cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
		
		
		s="christmas-carol.txt";
		
		lf=new LetterFrequency(s);
		lfB=new LetterFrequencyB(s);
		cArr=lf.topFrequentLetter();
		cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
		
		s="huck-finn.txt";
		
		lf=new LetterFrequency(s);
		lfB=new LetterFrequencyB(s);
		cArr=lf.topFrequentLetter();
		cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
		
		s="les-mis.txt";
		
		lf=new LetterFrequency(s);
		lfB=new LetterFrequencyB(s);
		cArr=lf.topFrequentLetter();
		cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
		
		s="metamorphosis.txt";
		
		lf=new LetterFrequency(s);
		lfB=new LetterFrequencyB(s);
		cArr=lf.topFrequentLetter();
		cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
		
		s="my-man-jeeves.txt";
		
		lf=new LetterFrequency(s);
		lfB=new LetterFrequencyB(s);
		cArr=lf.topFrequentLetter();
		cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
		
		s="pride-prejudice.txt";
		
		lf=new LetterFrequency(s);
		lfB=new LetterFrequencyB(s);
		cArr=lf.topFrequentLetter();
		cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
		
		s="tale-of-two-cities.txt";
		
		lf=new LetterFrequency(s);
		lfB=new LetterFrequencyB(s);
		cArr=lf.topFrequentLetter();
		cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
		
		s="tom-sawyer.txt";
		
		lf=new LetterFrequency(s);
		lfB=new LetterFrequencyB(s);
		cArr=lf.topFrequentLetter();
		cArrB=lfB.topFrequentLetter();
		System.out.println("Txt sample: "+s);
		System.out.println("The time for creating a new object for version A and B respectively are: "
				+lf.gettCreate()+", "+lfB.gettCreate());
		System.out.println("The time for adding a new object for version A and B respectively are: "
				+lf.gettAdd()+", "+lfB.gettAdd());
		System.out.println("The time for checking an object for version A and B respectively are: "
				+lf.gettCheck()+", "+lfB.gettCheck());
		System.out.println("The time for updating an object for version A and B respectively are: "
				+lf.gettUpdate()+", "+lfB.gettUpdate());
		System.out.println("The time for getting tops for version A and B respectively are: "
				+lf.gettGetTop()+", "+lfB.gettGetTop());
	}

}