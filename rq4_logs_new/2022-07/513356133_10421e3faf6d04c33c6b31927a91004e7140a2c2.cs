using System;
using System.IO;
using System.Text;
using System.Collections.Generic; 
namespace QuickTools
{       
            public class Reader
            {
                    public static class Stored
                    {
                        public static string RowData = null;
                        public static List<string> ListData = new List<string>();
                        public static byte[] BytesData; 
                    }
                    
                    /*
                        This only provides the Bytes thar were
                        stored in a string file 
                    */
                    public static byte[] ReadFileBytes(string file)
                    {
                  List<object> dataList = new List<object>();
                  byte[] dataBytes;

               
                       
                        using (var reader = new StreamReader(file))
                        {
                              /*
                                  // this is supposed to reade 
                                  // either an array of data 
                                  // or  any data in number decimal 
                                  // format and it has to return it in 
                                  // an array format aswell
                              */
                              //bool isNext = false;//
                              string line;
                              string currentValue = "";
                              while (reader.Peek() >= 0)
                              {
                                    line = reader.ReadLine();
                                    //Get.Wait(line);// for testing only  
                                    //this will loop over the entired line
                                    //to make sure that it takes only the
                                    // numbers from it 
                                    for (int Char = 0; Char < line.Length; Char++)
                                    {
                                          switch (Get.IsNumber(line[Char]))
                                          {
                                                case true:
                                                      currentValue += line[Char];
                                                      break;

                                                case false:
                                                      if (currentValue != "")
                                                      {
                                                            dataList.Add(currentValue);
                                                            currentValue = "";
                                                            // Get.Wait(data[Char]); 
                                                      }
                                                      break;
                                          }
                                          //Get.Green(data[Char]); 
                                    }

                                    // Get.Green(data[0]); 
                              }


                        }

                        dataBytes = new byte[dataList.Count];
                  // pass all the values to the dataBytes array 
                  try
                  {
                        for (int indexer = 0; indexer < dataBytes.Length; indexer++)
                        {
                              dataBytes[indexer] = Convert.ToByte(dataList[indexer]);
                              Stored.RowData += dataList[indexer] + ",";
                        }
                  }
                  catch(Exception e)
                  {
                       Color.Yellow("The Bytes where not on the correct format, more details :{ "+e+" }");      
                  }
                  // passing values from the bytes readed                 
                  Stored.BytesData = dataBytes;
                        if(dataList.Count <= 0)
                  {
                        Color.Yellow("Not Valid bytes to read where found"); 
                  }
                  return dataBytes;
                
                               
                    }
                     
                    public static string ReadFile(string file)
                    {
                        string data = null;
                  using (StreamReader reader = new StreamReader(file))
                  {
                        data = reader.ReadToEnd();
                        Stored.RowData = data;  
                  }

                  return data; 
                    }
                    
                
            }
}