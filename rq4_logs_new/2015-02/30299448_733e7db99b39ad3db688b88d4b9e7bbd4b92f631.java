package codeFumk;

public class HowMany {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HowMany hm = new HowMany();
//		 hm.howMany({"39508"}, "53070");
		String[] strArgs =  {"04824","40824","76013"};
		 hm.howMany(strArgs, "??8?4");

	}
	
	  public int howMany(String[] zipCodes, String message) {
		    return 0;
		  }

}


/*
CrackingTheCode

During the WW2, a key part of the war tactics, was to intercept enemy messages and extract useful information once the messages were decoded. 
One day, one Soviet morse code decoder, intercepted a message containing information about a zip code of the city that was going to be bombed.
The message was very noisy and while decoding, some numbers of the zip code were missing. 
Given the zip codes of all cities, your task is to find out how many match the zip code from the message.

Example:
  message = "12?3?"
  '?' - digit lost during the decoding, that can be any digit.

zipCodes = ["12834", "13123", "12636", "22222"]

There are two zip codes that match the message : "12834" and "12636". Therefore the answer is 2.

Input parameters:
  message - string, The decoded message.
  zipCodes - string array, containing all known zip codes.

Constraints:
  message - will have exactly 5 characters, each character will be either a digit or '?'.
  zipCodes - will have less than 50 strings, each containing exactly 5 digits.

Return value:
 int, the number of cities that are in danger of being bombed.
Class Name:
  CrackingTheCode

Method signature:
  public int howMany(String[] zipCodes, String message)

Test Case 1:
  howMany({"39508"}, "53070") = 0

Test Case 2:
  howMany({"31028","71424"}, "4???6") = 0

Test Case 3:
  howMany({"04824","40824","76013"}, "??8?4") = 2

Test Case 4:
  howMany({"41775","34357","64360","69550","10393","97062","14176","41898","76325"}, "76325") = 1

Test Case 5:
  howMany({"62775","63309","17430","61115","60768"}, "6????") = 4

Test Case 6:
  howMany({"93503","20821","43573"}, "?35?3") = 2

Test Case 7:
  howMany({"94551","25145","12947","30877","06398","28318","08720","19418","76705","13224","86545","85518","84600","42584","00224"}, "0????") = 3

Test Case 8:
  howMany({"79410","75802","55928","06883","38059","56176","40241","97351","20274","27051","93438","37215","74783","31903","35019","56767","17062","94096","83215","07640","04965","73976","04282","38106","92441","49557","33373","57287","89316","73038","05942","12860","75768","31965","78184","54675","73376","07167","32702","25924","64463","26358","37953"}, "38106") = 1

Test Case 9:
  howMany({"84903","23688","67599","28396","40442","52746","66868","13649","86201","55889","39601","69503","20494","70038","30351","38886","47716","51253","45311","01625","40255","09744","72747","46992","43135","10196","39269","65008","40558","76673","82520","40058","99048","47245","55803","91933","62636"}, "40?5?") = 3

Test Case 10:
  howMany({"14572","14579","14570","42452","14571","92854","14573","14576"}, "1457?") = 6
 * 
 */

