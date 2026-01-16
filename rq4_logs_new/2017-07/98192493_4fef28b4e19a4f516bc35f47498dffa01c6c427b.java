import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Statement;
import java.util.Random;

public class Company extends Entry implements Reader{
	private static String names[];
	public static int numComps = 0;
	public String getTable(){
		return "Company";
	}
	
	public void setup(Statement stmt) {
		
		String fileName = "CompanyNames";

        String line = null;
        
        names = new String[DataData.SKILLS];
		try {
            FileReader fileReader = 
                new FileReader(fileName);

            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);
            
            int i = 0;
            while((line = bufferedReader.readLine()) != null){
                names[i] = line;
                i++;
            }   

            bufferedReader.close();         
        }catch(Exception e){
            System.out.println("Error");                
        }
	}
	
	public static String getCompanyName(){
		return names[new Random().nextInt(numComps)];
	}
	public String value(){
		String out;
		numComps++;
		int ID = numComps;
		String name = getCompanyName();
		String description = Jackson.saySomething();
		int hiring = new Random().nextInt(2);
		
		out = "(" + ID + ",'" + name + "','" + description + "'," + hiring + ")";
		
		return out;
	}
}