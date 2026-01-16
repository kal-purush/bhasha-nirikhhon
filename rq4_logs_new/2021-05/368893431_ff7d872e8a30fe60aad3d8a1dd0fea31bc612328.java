package logging;

import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;

public class FileLogger implements ILogger {
    BufferedWriter file_log = null;

    public FileLogger(String file_name)
    {
        try
        {
         file_log = new BufferedWriter(new FileWriter(file_name));
        } catch (IOException e)
            {
                 e.printStackTrace();
            }
    }


    public void write(long parameter)
    {
        try
        {
            file_log.write(parameter + "\n");
        } catch (IOException e)
            {
                System.out.println("Error!");
                e.printStackTrace();
            }

    }


    public void write(String s)
    {
        try
        {
            file_log.write(s);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    public void write(Object ... value)
    {
        String s = "";

            for(Object i: value)
            {
                s = s + i.toString();
            }
            try
            {
                file_log.write(s);
            }
            catch (IOException e)
            {
            e.printStackTrace();
            }
    }


    public void close()
    {
        try
        {
            file_log.close();
        }
        catch (IOException e)
        {
                e.printStackTrace();
        }
    }

    public void writeTime(long parameter, TimeUnit time)
    {
        try {
            switch (time) {
                case Nano:
                    file_log.write(parameter + "Nano");
                    break;
                case Micro:
                    file_log.write(parameter / Math.pow(10, 3) + "Micro");
                    break;
                case Milli:
                    file_log.write(parameter / Math.pow(10, 6) + "Milli");
                    break;
                case Sec:
                    file_log.write(parameter / Math.pow(10, 9) + "Sec");
                    break;
            }
        }
            catch (IOException e){
                e.printStackTrace();
            }

    }

    
    public void writeTime(String s, long parameter, TimeUnit time)
    {
        try {
            switch (time) {
                case Nano:
                    file_log.write(s + "" + parameter + "Nano");
                    break;
                case Micro:
                    file_log.write(s + "" + parameter / Math.pow(10, 3) + "Micro");
                    break;
                case Milli:
                    file_log.write(s + "" + parameter / Math.pow(10, 6) + "Milli");
                    break;
                case Sec:
                    file_log.write(s + "" + parameter / Math.pow(10, 9) + "Sec");
                    break;
            }
        }
            catch (IOException e)
            {
            e.printStackTrace();
            }

    }


}