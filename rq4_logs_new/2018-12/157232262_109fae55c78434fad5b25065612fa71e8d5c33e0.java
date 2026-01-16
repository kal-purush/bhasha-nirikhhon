package gp_02_.pkg0910._canjuraoronawilliams;

public class executeGUI 
{
      public static void launchEXE()
    {
        String filePath = "C:/Users/martinem7407/Documents/visual studio 2015/Projects/WindowsFormsApplication1/WindowsFormsApplication1/bin/Debug/WindowsFormsApplication1.exe";
            try {
             
                Process p = Runtime.getRuntime().exec(filePath);
             
                } 
            catch (Exception e) {
                    e.printStackTrace();
                }
    }
}