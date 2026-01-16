using System;
using System.Collections.Generic;
using org.restcomm.connect.sdk.dotnet;
class MainClass
{
    public static void Main(string[] args)
    {
        //Login 
        Account akount = new Account("Enter your sid here", "Enter your authtoken here", "https://cloud.restcomm.com/restcomm/2012-04-24/");
        Console.WriteLine("logged in");
        //Creates application
        Application app = akount.CreateApplication("testappps");

        Console.WriteLine(app.Properties.Sid);

        //Get list of all application
        List<Application> applist = akount.GetApplicationList();
        //prints name of all 
        foreach (Application a in applist)
        {
            Console.WriteLine(a.Properties.FriendlyName);
        }
        //deletes application 
        app.Delete();
        Console.WriteLine("application deleted");
        Console.ReadLine();

    }
}
