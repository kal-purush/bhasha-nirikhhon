/*
Here is some info on Caesar Cipher's: https://en.wikipedia.org/wiki/Caesar_cipher
*/
using System;





namespace encryption
{
  class MainClass {

    public static void decryptPassword(string characters, string passwd, int key){
      char[] shiftedAlphabet = characters.ToCharArray();
      for (int a = 0; a < key; a++) {
        //The last letter is saved
        char lastLetter = shiftedAlphabet[shiftedAlphabet.Length - 1];
        for (int b = shiftedAlphabet.Length - 1; b > 0; b--) {
          //Every letter up to the last letter is swapped to the 
          shiftedAlphabet[b] = shiftedAlphabet[b - 1];
        }
        shiftedAlphabet[0] = lastLetter;
      }
      string shiftedString = string.Join("", shiftedAlphabet);
      char[] passwdArr = passwd.ToCharArray();
      char[] finalPasswdArr = new char[passwd.Length];
      bool invalid = false;
      for (int i = 0; i < passwd.Length; i++) {
        char passwdLetter = passwdArr[i];
        char upperPasswd = char.ToUpper(passwdLetter);
        int placement = shiftedString.IndexOf(upperPasswd);
        if (placement == -1) {
          invalid = true;
          break;
        }
        char newLetter = characters[placement];
        if (char.IsLower(passwdLetter)) {
          newLetter = char.ToLower(newLetter);
        }
        finalPasswdArr[i] = newLetter;
      }
      if (invalid == false) {
        string decryptedPasswd = string.Join("", finalPasswdArr);
        Console.WriteLine("Decrypted Password: {0}", decryptedPasswd);
      }
      else{
        Console.WriteLine("ENCRYPTED PASSWORD IS INVALID");
      }
    }

    public static void encryptPassword(string characters){
       //The following shifts the each letter in the alpheet by the amount (encryptionKey)
      float encryptionKey = new Random().Next(1, characters.Length-1);
      char[] shiftedAlphabet = characters.ToCharArray();
      for (int a = 0; a < encryptionKey; a++) {
        //The last letter is saved
        char lastLetter = shiftedAlphabet[shiftedAlphabet.Length - 1];
        for (int b = shiftedAlphabet.Length - 1; b > 0; b--) {
          //Every letter up to the last letter is swapped to the 
          shiftedAlphabet[b] = shiftedAlphabet[b - 1];
        }
        shiftedAlphabet[0] = lastLetter;
      }
      Console.WriteLine("Choose an all letter password: ");
      string passwd = Console.ReadLine();
      char[] passwdArr = passwd.ToCharArray();
      char[] finalPasswdArr = new char[passwd.Length];
      bool invalid = false;
      for (int i = 0; i < passwd.Length; i++) {
        char passwdLetter = passwdArr[i];
        char upperPasswd = char.ToUpper(passwdLetter);
        int placement = characters.IndexOf(upperPasswd);
        if (placement == -1) {
          invalid = true;
          break;
        }
        char newLetter = shiftedAlphabet[placement];
        if (char.IsLower(passwdLetter)) {
          newLetter = char.ToLower(newLetter);
        }
        finalPasswdArr[i] = newLetter;
      }
      if (invalid == false) {
        string encryptedPasswd = string.Join("", finalPasswdArr);
        Console.WriteLine("\nEncrypted Password: {0}\n(Your decryption key: {1})\n[Note: Copy the encryption and the Key and try decrypting]", encryptedPasswd, encryptionKey);
      }
      else{
        Console.WriteLine("\nINVALID ENTRY\n(PLEASE TRY AGAIN)");
      }
    }
    public static void Main (string[] args) {

      //In the following variable I initialize all valid characters for the user password
      string characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";//_!@#$%^&*()?1234567890";
      
      //First the user is asked what they want to do
      Console.WriteLine("Do you have a previous password to decrypt (Y or N): ");
      string isDecrypt = Console.ReadLine();
      if (string.Equals(isDecrypt, "Y")) {
        Console.WriteLine("What is your key: ");
        int key = Convert.ToInt32(Console.ReadLine());
        Console.WriteLine("What was the encrypted password: ");
        string passwd = Console.ReadLine();
        decryptPassword(characters, passwd, key);
      }
      else if (string.Equals(isDecrypt, "N")){
        encryptPassword(characters);
      }
      else {
        Console.WriteLine("\nINVALID ENTRY\n(PLEASE TRY AGAIN)");
      }
    }
  }
}