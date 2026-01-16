package PageObjectModel.AppiumFramework.ObjectRepository;

public class AddressScreen 
{	
public static String[]  newAddress = {"xpath","//*[@text='Add a New Address' or @text='Continue']","xpath","iosXpath","addressFullName"};	
public static String [] addressFullName = {"xpath","//*[@resource-id='enterAddressFullName']","xpath","iosXpath","addressFullName"};
public static String [] addressPhoneNumber = {"xpath","//*[@resource-id='enterAddressPhoneNumber']","xpath","iosXpath","addressPhoneNumber"};
public static String [] addressPostelcode = {"xpath","//*[@resource-id='enterAddressPostalCode']","xpath","iosXpath","addressPostelcode"};
public static String [] addressCity = {"xpath","//*[@resource-id='enterAddressCity']","xpath","iosXpath","addressCity"};
public static String [] stateOrRegion = {"xpath","//*[@resource-id='enterAddressStateOrRegion']","xpath","iosXpath","stateOrRegion"};
public static String [] addressLine1 = {"xpath","//*[@resource-id='enterAddressAddressLine1']","xpath","iosXpath","addressLine1"};
public static String [] addressLine2 = {"xpath","//*[@resource-id='enterAddressAddressLine2']","xpath","iosXpath","addressLine2"};
public static String [] addressLandmark = {"xpath","//*[@resource-id='enterAddressLandmark']","xpath","iosXpath","addressLandmark"};
public static String [] continueButton = {"xpath","//*[@text='Continue']","xpath","iosXpath","continueButton"};
}