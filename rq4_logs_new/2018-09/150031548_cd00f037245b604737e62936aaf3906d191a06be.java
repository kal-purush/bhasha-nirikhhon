package configandtools;

import org.openqa.selenium.WebDriver;

public class Inputs {

	
	WebDriver driver;
	
	//-----Constructor ----
	public Inputs(WebDriver driver){

        this.driver = driver;

           }
	
	 /*---------Dropbox Web URL ---------------*/
	public static String MAIN_URL = "https://www.dropbox.com/";  //Do Not Change the URL
	
	
  /*---------User Specific Areas to be filled ---------------*/
	
	 /*---------Credentials ---------------*/
	public static String LOGIN_USER = "test@gmail.com";	// Please delete the entry between double quotation marks and enter your own dropbox user name
	public static String LOGIN_PWD = "123456";				// Please delete the entry between double quotation marks and enter your own dropbox password
	
	
	 /*---------Shared Folder Test  ---------------*/
	public static String SharedFolderNm="pupy";	// Please delete the entry between double quotation marks and enter a new name for the folder to be shared----
	public static String EmailThatYouShareFolder ="test@gmail.com";  // Please delete the entry between double quotation marks and enter a valid email of person with whom that folder be shared----
	public static String NoteforSharedFolder="I created this folder for you, hope you lile it";  // If you wish, please delete the entry between double quotation marks and enter a note that will inform the receiver

	/*---------Create New Folder Test ---------------*/
	
	public static String NewFolderName="NewBee"; // Please delete the entry between double quotation marks and enter a new folder name
	
	
	/*---------File Upload Test and Change Avatar Test--------------*/

	// Please delete the entry between double quotation marks and write the path of your file to be uploaded
	public static String filelocation="D:\\Users\\myol\\Desktop\\deneme.docx";  
	
	 // Please delete the entry between double quotation marks and write the path of your avatar to be uploaded
	public static String profilphotolocation="D:\\Users\\myol\\Desktop\\avatar.png";
	
}