package hadoop;
import org.apache.hadoop.util.*;
import java.util.*;

public class definition
{
	String state[]=new String[100];
	String worker []=new String [100];
definition()
{ 
state[1]= "Andhra Pradesh";
state[2]= "Arunachal Pradesh (AR)";
state[3]= "Assam (AS)";
state[4]= "Bihar (BR)";
state[5]= "Chhattisgarh (CG)";
state[6]= "Goa (GA)";
state[7]= "Gujarat (GJ)";
state[8]= "Haryana (HR)";
state[9]= "Himachal Pradesh (HP)";
state[10]= "Jammu and Kashmir (JK)";
state[11]= "Jharkhand (JH)";
state[12]= "Karnataka (KA)";
state[13]= "Kerala (KL)";
state[14]= "Madhya Pradesh (MP)";
state[15]= "Maharashtra (MH)";
state[16]= "Manipur (MN)";
state[17]= "Meghalaya (ML)";
state[18]= "Mizoram (MZ)";
state[19]= "Nagaland (NL)";
state[20]= "Odisha(OR)";
state[21]= "Punjab (PB)";
state[22]= "Rajasthan (RJ)";
state[23]= "Sikkim (SK)";
state[24]= "Tamil Nadu (TN)";
state[25]= "Telangana (TS)";
state[26]= "Tripura (TR)";
state[27]= "Uttar Pradesh (UP)";
state[28]= "Uttarakhand (UK)";
state[29]= "West Bengal (WB)";
state[30]= "Missouri/MO";                             	

worker[1] = "Employee of a private for-profit company or business, or of an individual, for wages, salary or commissions";
worker[2] = "Employee of a private not-for-profit, tax-exempt, or charitable organization";
worker[3] = "Local government employee (city, county, etc.)";
worker[4] = "State government employee";
worker[5] = "Federal government employee";
worker[6] = "Self-employed in own not incorporated business, professional practice, or farm";
worker[7] = "Self-employed in own incorporated business, professional practice or farm";
worker[8] = "Working without pay in family business or farm";
worker[9] = "Unemployed and last worked 5 years ago or earlier or never worked";

}	
String get_state(int i)
{
	return state[i];
}

String get_worker(int i)
{
	return worker[i];
}

String get_marital_status(int i)
{
	String key_name="";
	
	if(i==1)
	key_name="Married";
	else if(i==2)
	key_name="Widowed";
	else if(i==3)
	key_name="Divorced";
	else if(i==4)
	key_name="Separated";
	else if(i==5)
	key_name="Never married";
	return key_name;
}

String get_religion(int i)
{
	String key_name="";
	
	if(i==1)
	key_name="Hinduism";
	else if(i==2)
	key_name="Islam";
	else if(i==3)
	key_name="Christianity";
	else if(i==4)
	key_name="Sikhism";
	else if(i==5)
	key_name="0thers";
	return key_name;
}




String get_mother_tongue(int i)
{
	String key_name="";
	
	if(i==1)
	key_name="HINDI";
	else if(i==2)
	key_name="English";
	else if(i==3)
	key_name="Bengali";
	else if(i==4)
	key_name="Telugu";
	else if(i==5)
	key_name="Marathi  ";
	return key_name;
}

String get_Educational_attainment(int i)
{
	String key_name="";
	
	if(i==1)
	key_name="No schooling completed";
	else if(i==2)
	key_name="Nursery school, preschool";
	else if(i==3)
	key_name="Kindergarten";
	else if(i==4)
	key_name="Grade 1";
	else if(i==5)
	key_name="Grade 2";
     else if(i==6)
	key_name="Grade 3";
	else if(i==7)
	key_name="Grade 4";
	else if(i==8)
	key_name="Grade 5";
	else if(i==9)
	key_name="Grade 6";
	else if(i==10)
	key_name="Grade 7";
     else if(i==11)
	key_name="Grade 8";
	else if(i==12)
	key_name="Grade 9";
	else if(i==13)
	key_name="Grade 10";
	else if(i==14)
	key_name="Grade 11";
	else if(i==15)
	key_name="12th grade - no diploma";
     else if(i==16)
	key_name="Regular high school diploma";
	else if(i==17)
	key_name="GED or alternative credential";
	else if(i==18)
	key_name="Some college, but less than 1 year";
	else if(i==19)
	key_name="Associate's degree";
	else if(i==20)
	key_name="Bachelor's degree";
     else if(i==21)
	key_name="Master's degree";
	else if(i==22)
	key_name="Nursery school, preschool";
	else if(i==23)
	key_name="Professional degree beyond a bachelor's degree";
	else if(i==24)
	key_name="Doctorate degree";
	return key_name;

}

String get_Health_insurance(int i)
{
	String key_name="";
	
	if(i==1)
	key_name="With health insurance coverage";
	else if(i==2)
	key_name="No health insurance coverage";
	return key_name;
}





}
