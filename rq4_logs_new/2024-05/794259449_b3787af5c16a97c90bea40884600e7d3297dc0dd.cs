using Microsoft.AspNetCore.Identity;
using Xunit;

namespace WebApp;

public static class Utils
{
    public static int SumInts(int a, int b){
        return a + b;
    }


        public static bool IsPasswordGoodEnough(string password){
    Regex passwordCheck = new Regex("^(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])(?=.*?[#?!@$%^&*-]).{8,}$");
    return passwordCheck.IsMatch(password);

    
}

    }
    


    //Till­räck­ligt svårt lösenord. En metod för att kon­trol­lera att ett lösenord är svårt nog att gissa. 
    //Det ska vara minst 8 tec­ken långt, inne­hål­ler både små och stora bok­stä­ver, minst en siffra, 
    //samt minst ett annat tec­ken. Meto­dens inpa­ra­me­ter ska vara en sträng, lösenor­det, 
    //och den ska retur­nera en boo­lean – true om lösenord är god­känt, annars false. 
    //Döp meto­den till IsPas­sword­GoodE­nough.



/*    public static Arr CreateMockUsers()
{
  var read = File.ReadAllText(Path.Combine("json/mock-users.json"));  
       Arr mockUsers = JSON.Parse(read);
       Arr successFullyWrittenUsers = Arr();

       foreach(var user in mockUsers)
       {
        user.password = "12345678";
       var result = SQLQueryOne(
        @"INSERT INTO users(firstName,lastName,email,password)
        VALUES($firstName,$lastName,$email,$password)
       ", user);
       //If we get an error from the DB then we haven't added
       //the mock users, if not we have so add to the successful list
       if(!result.HasKey("error")){
        user.Delete("password"); 
       successFullyWrittenUsers.Push(user); 
       };
       }
       return successFullyWrittenUsers;
}
*/
