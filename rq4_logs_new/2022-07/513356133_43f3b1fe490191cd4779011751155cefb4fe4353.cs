/*
This Contains all the shortcuts for the Alerts
and events of colors for the display of the 
text. 
important Date : today i have change the class name from "Do static class"
to "Get static class" due to some not sence ideas behing the idea of doing stuff
it is nothing related with the performance but more with relation with the 
action that it creates DATE OF UPDATE : 03/11/2022
*/
using System;
using System.Threading; 
using System.IO;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace QuickTools
{




		///////////////////////////////////////////////////////////
		///////////////////////////////////////////////////////////
		/////////////////*this is were the Class Get Starts *//////
		///////////////////////////////////////////////////////////
		///////////////////////////////////////////////////////////
	
		public  class Get:Color
		{


            public static void WaitTime(int sleepTime)
            {

                  try
                  {
                        Thread.Sleep(sleepTime);

                  }catch(Exception e )
                  {
                        Get.Wrong(e); 
                  }


            }

            public static void _(int sleepTime)
            {
                  if (sleepTime < 12000)
                  {
                        Thread.Sleep(sleepTime);
                  }
            }

            public static string Path = Directory.GetCurrentDirectory();
            public static string DataPath = Directory.GetCurrentDirectory() + "/data/qt/"; 

            // this generate a random byte 
            // between 0 to 250 
            // which could be use to create a 
            // key 
            static public byte RandomByte()
            {
                  Random rand = new Random();
                  int min = 0;
                  int max = 250;

                  int number = rand.Next(min, max);
                  byte finalNumber = Convert.ToByte(number);
                  return finalNumber;

            }
            public static byte[] KeyGenerated = new byte[16]; 

            /*
                  This method generate a 16 bits
                  key and it is sored either in the array 
                  or is stored in a keys.secure file 
            */
            public static byte[] RandomByteKey()
            {
                  byte[] key = new byte[16];

                  for (int index =0; index<key.Length; index++)
                  {
                        key[index] = RandomByte(); 
                  }
                  KeyGenerated = key; 
                  return key; 
            }

            public static byte[] RandomByteKey(bool autoSave)
            {
                  // if is not added the bool type will just return 
                  // a full of 0 byes
                  byte[] key = new byte[16];

                  if (autoSave == true)
                  {
                       
                        for (int index = 0; index < key.Length; index++)
                        {
                              key[index] = RandomByte();
                        }
                        KeyGenerated = key;
                  }
                  SaveKey(); 
                  return key;
            }
            /*
            SaveKey 
            basically is incharge of 
            making sure that the thata is saved 
            inside QuickTools data folder 
            so if is not created 
            it will create one to make sure that is saved
            properly 
            
                  */
            public static void SaveKey()
            {
                  string path, keyFile, qtDir; 
                   path = Directory.GetCurrentDirectory();
                   qtDir = "data/qt/";
                   keyFile = "data/qt/secure.key";
                  if (Directory.Exists(qtDir) == false)
                  {             
                        Directory.CreateDirectory(qtDir);
                        SaveKey();       
                  }
                  else
                  {
                       
                       
                        using (FileStream file = File.Create(keyFile))
                        {

                              using (StreamWriter writer = new StreamWriter(file))
                              {
                                    foreach (byte key in KeyGenerated)
                                    {
                                          writer.Write(key + ",");
                                    }
                              }
                        } 

                       
                  }

                   
            }





            /*
  this check will veryfi if a name has an invalid
  character in order for it to return a valid name 

  */
            /*
/////////////////////////////////////////////////////////////////////////////
////////////     THIS AREA CONTROLS CONSOLE VALIDATION  /////////////////////
/////////////////////////////////////////////////////////////////////////////
*/


            public static bool Check()
			{
				// Regex rueles for the allowed name types 
				var rules = new Regex("^[a-zA-Z0-9_]*$");
                  //conditions with an input 
                  // this basically just get the return 
                  // from the  the input and it convers it to 
                  // text to validate if it match the rules 
                               
				
				      // if the name is blank is not a valid name actually 
				if (rules.IsMatch(Get.Input().ToString()) && Get.input.ToString().Length > 0)
				{
                        //Valid Name


                        return true; 
				}
				else
				{
					// invalid Name 
					return false;
				}
			}
			
     
            /*
/////////////////////////////////////////////////////////////////////////////
//////////// THIS AREA CONTROLS CONSOLE Profile  /////////////////////////////
/////////////////////////////////////////////////////////////////////////////
*/
            public static void Login()
			{
				string name, password;
				Get.Box("Singup", 4);
				Get.Yellow("Type Your Information Please.", 3);
				Get.Box("Name", 1);
				name = Get.TextInput();
				Get.Box("Password");
				Get.Red("Hidden For Privacy and Security");
				Get.Hide();
				password = Console.ReadLine();
				Get.Reset();
#pragma warning disable RECS0026 // Possible unassigned object created by 'new'
                  new User(name, password);
#pragma warning restore RECS0026 // Possible unassigned object created by 'new'
            }

            public static void SingUp()
			{
				string name, lastName, password, dob, phone, email;
				Get.Box("Singup", 4);
				Get.Yellow("Type Your Information Please.", 3);
				Get.Box("Name", 1);
				name = Get.TextInput();
				Get.Box("Last Name", 1);
				lastName = Get.TextInput();
				Get.Box("Date of Birth", 1);
				Get.Red("Hidden For Privacy and Security");
				Get.Hide();
				dob = Console.ReadLine();
				Get.Reset();
				Get.Box("Phone Number", 1);
				phone = Get.TextInput();
				Get.Box("Email Address", 1);
				email = Get.TextInput();
				Get.Box("Password");
				Get.Red("Hidden For Privacy and Security");
				Get.Hide();
				password = Console.ReadLine();
				Get.Reset();
			    new User(name, lastName, password, dob, phone, email);
			//  Get.Wait(name+" "+lastName + " " +password + " " +dob + " " +phone + " " +email);
			}

			/*
      /////////////////////////////////////////////////////////////////////////////
      //////////// THIS AREA CONTROLS CONSOLE INPUT   /////////////////////////////
      /////////////////////////////////////////////////////////////////////////////
      */
			public static string Text = "";
			public static int Number = 0;
			public static object input = "";
			// this is the Input method which will retrun a number or text and 
			// it just need to be treated as such 
			public static string Key = "";
			           
			public static object KeyInput()
			{
			//	Get.LabelSide(">"); // just looks better damme it 
			//	Console.Write(" ");
			// it just did not make as much sence on 
			//why would i want something that is pointing to 
			// nothing because is just wating for a key to be pressed
				var InputKey = Console.ReadKey();
				Get.Clear();
				// this will return a type of number or key 
				// with the information from it like 1D1
				string inputValue = InputKey.Key.ToString();
				Key = inputValue;
				return inputValue;
			}
            public static int KeyNumber()
            {
                  //    Get.LabelSide(">"); // just looks better damme it 
                  //    Console.Write(" ");
                  // it just did not make as much sence on 
                  //why would i want something that is pointing to 
                  // nothing because is just wating for a key to be pressed
                  var InputKey = Console.ReadKey();
                  Get.Clear();
                  int inputValue = 0000;// this has to be disabled 
                                   
                  if (InputKey.Key.ToString().Length > 1)
                  {
                        try
                        {
                              string input = InputKey.Key.ToString();
                              string lastDigit = input[input.Length - 1].ToString();
                              inputValue = int.Parse(lastDigit);
                        }
                        catch(Exception e)
                        {
                              Get.Alert(e);           
                        }
                  }
                  else
                  {
                        Get.WrongInput(InputKey);                        
                  }

                  // this will return a type of number or key 
                  // with the information from it like 1D1

                     Key = inputValue.ToString(); // just in case                 
                     Number = inputValue;          // if someone just rather to  use the key 
                  return inputValue;
            }






            public static string InputChar = ">"; 
			public static object Input()
			{
				Get.LabelSide(InputChar);
				//Console.Write(" ");
				bool isnumber;
				string input = Console.ReadLine();
				int number;
				                 
				isnumber = int.TryParse(input, out number);
				
				if (isnumber)
				{
					Number = number;
					Get.input = number;
					return number;
				}
				else
				{
					Get.input = input;
					Get.Text = input;
					return input;
				}
			}
            public static object Input(object display)
            {
                  Get.Write(display);                 
                  Get.LabelSide(InputChar);
                  //Console.Write(" ");
                  bool isnumber;
                                         
                  string input = Console.ReadLine();
                  int number;
                  isnumber = int.TryParse(input, out number);
                  if (isnumber)
                  {
                        Number = number;
                        Get.input = number;
                        return number;
                  }
                  else
                  {
                        Get.input = input;
                        Get.Text = input;
                        return input;
                  }
            }
            public static bool IsNumber(object input)
            {
                        string textInput = input.ToString();
                        int number = 0; 
                        bool isNumber = false; 
                        
                        isNumber = int.TryParse(textInput.Replace("D",""),out number);
                       
                      
                       
                       if(isNumber == true)
                        {
                            return true; 
                        }else{
                            return false; 
                        }
            }
			public static int NumberInput()
			{
			                 
				Get.LabelSide(">");
				Console.Write(" ");
				bool isnumber = false;
#pragma warning disable RECS0117 // Local variable has the same name as a member and hides it
                  string input = Console.ReadLine();
#pragma warning restore RECS0117 // Local variable has the same name as a member and hides it
                  int number;
				isnumber = int.TryParse(input, out number);
				if (isnumber)
				{
					Number = number;
                        return number;
                  }
                  else
                  {
                        Get.WrongInput();                        
                        return 0;                       
                  }

            }

			public static string TextInput()
			{
				Get.LabelSide(">");
				Console.Write(" ");
				string text = Console.ReadLine();
				Text = text;
				return text;
			}

			public static void Reset()
			{
				Console.ResetColor();
			}

			public static void Cle()
			{
				Console.Clear();
			}

			public static void Clear()
			{
				Console.Clear();
			}

		

			public static void Label(object msg)
			{
				Console.BackgroundColor = ConsoleColor.Magenta;
				Console.ForegroundColor = ConsoleColor.White;
				Console.Write(" " + msg + " ");
                  Get.W("");                 
				Get.Reset();
			}

			public static void LabelSide(object msg)
			{
				Console.BackgroundColor = ConsoleColor.Magenta;
				Console.ForegroundColor = ConsoleColor.White;
				Console.Write("\n" + msg + " ");
				Get.Reset();
			}

			public static void Title(object msg)
			{
				Console.WriteLine("\t" + msg);
			}

			// this gives you the avility to 
			// deside how many tabs you want to include
			// in the title 
			public static void Title(object msg, int tabs)
			{
				string spaces = "\t";
				string tabSpaces = "";
				for (int i = 0; i <= tabs; i++)
				{
					tabSpaces += spaces;
				}

				Console.WriteLine(tabSpaces + msg);
			}

			// Default box 
			public static void Box(object content)
			{
				string simbol = "/";
				string underLine = "";
				for (int i = 0; i <= content.ToString().Length + 3; i++)
				{
					underLine += simbol;
				}

				Get.W(underLine);
				Get.W(simbol + " " + content + " " + simbol);
				Get.W(underLine);
			}
            public static void Box(object content,object simbol)
            {
                 
                  string underLine = "";
                  for (int i = 0; i <= content.ToString().Length + 3; i++)
                  {
                        underLine += simbol;
                  }

                  Get.W(underLine);
                  Get.W(simbol + " " + content + " " + simbol);
                  Get.W(underLine);
            }
            // with tabs 
            public static void Box(object content, int tabs)
			{
				string spaces = "\t";
				string tabSpaces = "";
				for (int i = 0; i <= tabs; i++)
				{
					tabSpaces += spaces;
				}

				string simbol = "/";
				string underLine = "";
				for (int i = 0; i <= content.ToString().Length + 3; i++)
				{
					underLine += simbol;
				}

				Get.W(tabSpaces + underLine);
				Get.W(tabSpaces + simbol + " " + content + " " + simbol);
				Get.W(tabSpaces + underLine);
			}

			// custom box 
			public static void Box(object content, string simbol, int tabs)
			{
				string spaces = "\t";
				string tabSpaces = "";
				for (int i = 0; i <= tabs; i++)
				{ // you just want to add tabs 
					// if there is tabs added 
					if (tabs > 0)
					{
						tabSpaces += spaces;
					}
				}

				string underLine = "";
				for (int i = 0; i <= content.ToString().Length + 3; i++)
				{
					underLine += simbol;
				}

				Get.W(tabSpaces + underLine);
				Get.W(tabSpaces + simbol + " " + content + " " + simbol);
				Get.W(tabSpaces + underLine);
			}

			public static void Wrong()
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine("Something Was Wrong!!!");
				Console.ResetColor();
				Get.Wait();
			}

			public static void Wrong(object text)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine("Something Was Wrong!!!");
				Console.WriteLine("This" + ":=>>>> " + text);
				Console.ResetColor();
				Get.Wait();
			}

			public static void WrongIn()
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine(" Wrong Input!!!");
				Console.ResetColor();
				Get.Wait();
			}

			public static void WrongIn(object msg)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine(" Wrong Input!!!, this is not a valid input: '{0}' ", msg);
				Console.ResetColor();
				Get.Wait();
			}

			public static void WrongInput(object msg)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine(" Wrong Input!!!, this is not a valid input: '{0}' ", msg);
				Console.ResetColor();
				Get.Wait();
			}
            public static void WrongInput()
            {
                  // if one of the inputs is null it will return the other                  
                  string msg = Get.Text.ToString() != "" ? Get.Text.ToString() : Get.input.ToString();                     
                  Console.ForegroundColor = ConsoleColor.Red;
                  Console.WriteLine(" Wrong Input!!!, this is not a valid input: '{0}' ", msg );
                  Console.ResetColor();
                  Get.Wait();
            }

            public static void NotFound()
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine("File Not Founded");
				Console.ResetColor();
				Get.Wait();
			}

			public static void NotFound(object msg)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine("This File Was Not Founded '{0}' ", msg);
				Console.ResetColor();
				Get.Wait();
			}

			public static void Wait()
			{
				Get.W("Type any key to continue");
				Console.ReadKey();
			}

			// just for testin porpuses
			public static void Wait(object Caller)
			{
				Get.W("Type any key to continue");
				Get.Yellow("This Called me =>  '" + Caller + "'");
				Console.ReadKey();
			}


			public static void Ok()
			{
				Console.ForegroundColor = ConsoleColor.Green;
				Console.WriteLine("OK");
				Console.ResetColor();
				Get.Wait();
			}

			public static void Good(object msg)
			{
				Console.ForegroundColor = ConsoleColor.Green;
				Console.WriteLine(msg);
				Console.ResetColor();
			}

			public static void Bad()
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Get.Wait();
			}

			public static void Bad(object msg)
			{
				Console.ForegroundColor = ConsoleColor.Green;
				Console.WriteLine(msg);
				Console.ResetColor();
				Get.Wait();
			}

			public static void Alert()
			{
				Console.ForegroundColor = ConsoleColor.Yellow;
			}

			public static void Alert(object msg)
			{
				Console.ForegroundColor = ConsoleColor.Yellow;
				Console.WriteLine(msg);
				Console.ResetColor();
				Get.Wait();
			}

			public static void Hide()
			{
				Console.ForegroundColor = ConsoleColor.Black;
				Console.BackgroundColor = ConsoleColor.Black;
			}

			public static void HideText()
			{
				Console.ForegroundColor = ConsoleColor.Black;
				Console.BackgroundColor = ConsoleColor.Black;
			}

            /*
  /////////////////////////////////////////////////////////////////////////////
  //////////// THIS AREA CONTROLS CONSOLE OUTPUT  /////////////////////////////
  /////////////////////////////////////////////////////////////////////////////
  */
            // quick WriteLine just as a quick method while im using an online editor 
            public static void Wl(object text)
            {
                  Console.Write(text);
            }

            public static void W(object text)
			{
				Console.WriteLine(text);
			}

			public static void Write(object text)
			{
				Console.WriteLine(text);
			}

			public static void C(object text)
			{
				Console.WriteLine(text);
			}

			// here are the methods to write with tabs 
			public static void W(object text, int tabs)
			{
				string spaces = "\t";
				string tabSpaces = "";
				for (int i = 0; i <= tabs; i++)
				{
					tabSpaces += spaces;
				}

				Console.WriteLine(tabSpaces + text);
			}

			public static void Write(object text, int tabs)
			{
				string spaces = "\t";
				string tabSpaces = "";
				for (int i = 0; i <= tabs; i++)
				{
					tabSpaces += spaces;
				}

				Console.WriteLine(tabSpaces + text);
			}
            public static void Wl(object text, int tabs)
            {
                  string spaces = "\t";
                  string tabSpaces = "";
                  for (int i = 0; i <= tabs; i++)
                  {
                        tabSpaces += spaces;
                  }

                  Console.WriteLine(tabSpaces + text);
            }

            public static void C(object text, int tabs)
			{
				string spaces = "\t";
				string tabSpaces = "";
				for (int i = 0; i <= tabs; i++)
				{
					tabSpaces += spaces;
				}

				Console.WriteLine(tabSpaces + text);
			}

			/*
           here im just going to add some shortcuts for the console colors
           sadly it will be for type string only 
         */
			/*
          /////////////////////////////////////////////////////////////////////////////
          //////////// THIS AREA CONTROLS CONSOLE JUST DOCUMENT INFOMATION ////////////
          /////////////////////////////////////////////////////////////////////////////
          */
			public static string Version = "A032022";
			private static string lastModified = "03/23/2022";
			public static void About()
            {

                  Green();
                  Console.WriteLine(@"                                                

                                                                                                                                                                                        
                                                                                                                                                                                        
     QQQQQQQQQ                          iiii                      kkkkkkkk           TTTTTTTTTTTTTTTTTTTTTTT                                                   lllllll                  
   QQ:::::::::QQ                       i::::i                     k::::::k           T:::::::::::::::::::::T                                                   l:::::l                  
 QQ:::::::::::::QQ                      iiii                      k::::::k           T:::::::::::::::::::::T                                                   l:::::l                  
Q:::::::QQQ:::::::Q                                               k::::::k           T:::::TT:::::::TT:::::T                                                   l:::::l                  
Q::::::O   Q::::::Q uuuuuu    uuuuuu  iiiiiii     cccccccccccccccc k:::::k    kkkkkkkTTTTTT  T:::::T  TTTTTT   ooooooooooo      ooooooooooo      ooooooooooo    l::::l     ssssssssss   
Q:::::O     Q:::::Q u::::u    u::::u  i:::::i   cc:::::::::::::::c k:::::k   k:::::k         T:::::T         oo:::::::::::oo  oo:::::::::::oo  oo:::::::::::oo  l::::l   ss::::::::::s  
Q:::::O     Q:::::Q u::::u    u::::u   i::::i  c:::::::::::::::::c k:::::k  k:::::k          T:::::T        o:::::::::::::::oo:::::::::::::::oo:::::::::::::::o l::::l ss:::::::::::::s 
Q:::::O     Q:::::Q u::::u    u::::u   i::::i c:::::::cccccc:::::c k:::::k k:::::k           T:::::T        o:::::ooooo:::::oo:::::ooooo:::::oo:::::ooooo:::::o l::::l s::::::ssss:::::s
Q:::::O     Q:::::Q u::::u    u::::u   i::::i c::::::c     ccccccc k::::::k:::::k            T:::::T        o::::o     o::::oo::::o     o::::oo::::o     o::::o l::::l  s:::::s  ssssss 
Q:::::O     Q:::::Q u::::u    u::::u   i::::i c:::::c              k:::::::::::k             T:::::T        o::::o     o::::oo::::o     o::::oo::::o     o::::o l::::l    s::::::s      
Q:::::O  QQQQ:::::Q u::::u    u::::u   i::::i c:::::c              k:::::::::::k             T:::::T        o::::o     o::::oo::::o     o::::oo::::o     o::::o l::::l       s::::::s   
Q::::::O Q::::::::Q u:::::uuuu:::::u   i::::i c::::::c     ccccccc k::::::k:::::k            T:::::T        o::::o     o::::oo::::o     o::::oo::::o     o::::o l::::l ssssss   s:::::s 
Q:::::::QQ::::::::Q u:::::::::::::::uui::::::ic:::::::cccccc:::::ck::::::k k:::::k         TT:::::::TT      o:::::ooooo:::::oo:::::ooooo:::::oo:::::ooooo:::::ol::::::ls:::::ssss::::::s
 QQ::::::::::::::Q   u:::::::::::::::ui::::::i c:::::::::::::::::ck::::::k  k:::::k        T:::::::::T      o:::::::::::::::oo:::::::::::::::oo:::::::::::::::ol::::::ls::::::::::::::s 
   QQ:::::::::::Q     uu::::::::uu:::ui::::::i  cc:::::::::::::::ck::::::k   k:::::k       T:::::::::T       oo:::::::::::oo  oo:::::::::::oo  oo:::::::::::oo l::::::l s:::::::::::ss  
     QQQQQQQQ::::QQ     uuuuuuuu  uuuuiiiiiiii    cccccccccccccccckkkkkkkk    kkkkkkk      TTTTTTTTTTT         ooooooooooo      ooooooooooo      ooooooooooo   llllllll  sssssssssss    
             Q:::::Q                                                                                                                                                                    
              QQQQQQ                                                                                                                                                                    
                                                                                                                                                                                        
                                                                                                                                                                                        
                                                                                                                                                                                        


");

            //	Get.Label("QuickTools Version: " + version);
            Get.W("Created By MBV");
				Get.W("Last Update: " + lastModified);
				Get.Wait();
                  LastChanges();                
			}

			public static void LastChanges()
			{
				List<string> changes = new List<string>();
				changes.Add("3/21/2022 update the nulls value that was required to remove them.");
				changes.Add("updates on the colors and sides functions 'LabelSide added which will give a color and will have an space on the top.'");
				changes.Add("3/23/2022 Today i will add the profile method and also the Designe of the pink color of the inputs");
				changes.Add("5/23/2022 Today i will be retaking the project and deviding the system into lyers for it to be esier to grow ");
				for (int change = 0; change < changes.Count; change++)
				{
					int number = change + 1;
					Get.Yellow(number + ". " + changes[change]);
				}

				Get.Wait();
			}
		}
	}