import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.StringReader;
import com.google.gson.stream.JsonReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.io.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.lang.String;
public class Main {
    //глобальные переменные

    private static String global_string="";
    private static String Final_String="";//Строка,которая финальная
    private static Scanner in = new Scanner(System.in);
    private static List dir_list=new ArrayList();//список со всеми файлами в папке
    private static   JsonParser jsonParser = new JsonParser();


    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Введите функцию для поиска");
String str ='"'+reader.readLine()+'"';
Final_String+=str+"=>";
        System.out.println("Введите папку для поиска");
        String file =reader.readLine();
//+"create_agreement"+'"';
        //"C:\\Users\\leged\\OneDrive\\Рабочий стол\\domru\\agreementsJSON"
        File currentDir = new File(file);
        displayAllFilesDirectories(currentDir);
Recurs(str);
System.out.println("Конечное дерево");
System.out.println(Final_String);

    }
//функция для одного имени
    public static void Recurs(String str) throws IOException {
        //список храняший все адреса и названия .json документоv

        for (int i=0;i<dir_list.size();i++){
            String adress=dir_list.get(i).toString();
            Old_main(adress,str);
           System.out.println(i);
        }
    }


    private static void Old_main(String str,String str2) throws IOException {
        //C:/Users/leged/OneDrive/Рабочий стол/domru/agr_flag_city_link_triggers.json
        String jsonString = str;
        String file = readUsingFiles(jsonString);
        int count =0;
        StringBuffer sb = new StringBuffer(file);
        String string_for_searh=str2;
                //"\"create_agreement\"";
        //Узнаем количество символов в имени того что мы ищем
        int number = string_for_searh.length();


        int number_inside=count_string(file,string_for_searh);

        for (int i=0;i<number_inside;i++)
        {
            System.out.println(count_string(file,string_for_searh)+" "+string_for_searh+"Упоминаний в документе:"+str);
            global_string+="parserRule"+"=> ";

            JsonObject jsonObject = jsonParser.parse(file).getAsJsonObject();
            JsonArray arr = jsonObject.getAsJsonArray("parserRule");
            //первый вызов рекурсивной функции
            Way(arr,string_for_searh);
            //Для удаления ненужной подстроки
            int first=file.indexOf(string_for_searh);
            sb.delete(first+1,first+number-1);
            file=sb.toString();

            System.out.println(global_string);
          //  Find_a_Lower(global_string);//поиск уровня
            if ((global_string.indexOf("function_call")>0||global_string.indexOf("procedure_call")>0)&&(global_string.indexOf("function_body")>0||global_string.indexOf("procedure_body")>0))
            {

                System.out.println(global_string);
                int f=global_string.indexOf("(");
                int l=global_string.indexOf(")");
                String str_final="";
                int length=l-f;
                char[] Arr=new char[length-1];
                try {
                    global_string.getChars(f+1, l, Arr, 0);
                    //  System.out.print("Скопированное значение: " );

                    str_final=String.valueOf(Arr);

                    System.out.println(str_final);

                } catch (Exception ex) {
                    System.out.println("Возникает исключение...");
                }

                if (str_final.length()>1)
                {
                    String str_for_Recurs='"'+str_final+'"';
                    if (str2.equals(str_for_Recurs))
                    {


                    }
                    else{
                        Final_String+=str_for_Recurs+"=>";
                        Recurs(str_for_Recurs);
                    }

                }

            }
            global_string="";
        }
        //System.out.println(Final_String);

    }
    private static String readUsingFiles(String fileName) throws IOException {
        return new String(Files.readAllBytes(Paths.get(fileName)));
    }
    private static JsonArray Way(JsonArray arr,String str) {
        int that_we_need=-1;
        String str_final=" ";
        for (int i=0;i<arr.size();i++)
        {

            String post_id = arr.get(i).toString();
            int number = post_id.indexOf(str);
            if (number>0&&that_we_need==-1  ){
                that_we_need=i;
            }

        }
        if (that_we_need==-1){
            //Поменять на булевую переменную
            System.out.println("Данной функции или процедуры не было найдено");
        }else
        {
            //Нахождениня названия следующей части
            String find=arr.get(that_we_need).toString();
            int first=find.indexOf('"');
            int second=find.indexOf('"',first+1);
            int third=second-first-1;
            char[] Str1 = new char[third];

            try {
                find.getChars(first+1, second, Str1, 0);
              //  System.out.print("Скопированное значение: " );

                str_final=String.valueOf(Str1);
               // System.out.println(str_final);
                global_string+=str_final+"=> ";

            } catch (Exception ex) {
                System.out.println("Возникает исключение...");
            }
        }
        if (str_final.equals("type"))
        {
            boolean ok=false;
            ok=Chek_call(global_string);
            if(ok==true){
              //  Final_String+=global_string;
            }else
            {
                ok=false;
            }


        }else{
            String find = arr.get(that_we_need).toString();
            //Функции на поиск имени
            Output_name_Body(str_final,find);
            JsonObject jsonObject = new JsonParser().parse(find).getAsJsonObject();
            JsonArray arr2 = jsonObject.getAsJsonArray(str_final);
           // System.out.println(global_string);
            Way(arr2,str);
        }

        return new JsonArray();
    }
    //подсчет сколько раз строка входит в файл
    public static int count_string(String str, String target) {
        return (str.length() - str.replace(target, "").length()) / target.length();
    }
    //Проверка на есть ли вызов функции внутри дерева
    private static boolean Chek_call(String str){
        boolean ok=false;
        String function_call="function_call";
        String procedure_call="procedure_call";
        if(str.indexOf(function_call)>0||str.indexOf(procedure_call)>0)
        {
            ok=true;
        }else{
            ok=false;
        }
        return ok;
    }
    //str=>строка с именем блока
    //str2=>строка с текстовым блоком
    private static void Output_name_Body(String str,String str2){
        String str_final="";
    if (str.equals("procedure_body")||str.equals("function_body")){
        int first=str2.indexOf("text");
        int second=str2.indexOf("text",first+1);//второй text
        int third=str2.indexOf('"',second+7);
        int length = third-second;
        char[] Arr=new char[length-7];
        try {
            str2.getChars(second+7, third, Arr, 0);
            //  System.out.print("Скопированное значение: " );

             str_final=String.valueOf(Arr);

            global_string+="("+str_final+") ";

        } catch (Exception ex) {
            System.out.println("Возникает исключение...");
        }

    }else {

    }
        if (str.equals("procedure_call")||str.equals("function_call")){
            int first=str2.indexOf("text");
            int third=str2.indexOf('"',first+7);
            int length = third-first;
            char[] Arr=new char[length-7];
            try {
                str2.getChars(first+7, third, Arr, 0);
                //  System.out.print("Скопированное значение: " );

                str_final=String.valueOf(Arr);

                global_string+="("+str_final+") ";

            } catch (Exception ex) {
                System.out.println("Возникает исключение...");
            }

        }else {

        }

    }
    private static String getFileExtension(File file) {
        String fileName = file.getName();
        // если в имени файла есть точка и она не является первым символом в названии файла
        if(fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0)
            // то вырезаем все знаки после последней точки в названии файла, то есть ХХХХХ.txt -> txt
            return fileName.substring(fileName.lastIndexOf(".")+1);
            // в противном случае возвращаем заглушку, то есть расширение не найдено
        else return "";
    }
    public static void displayAllFilesDirectories(File dir) {
        try {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                  //  System.out.println("directory: " + file.getCanonicalPath());
                    // Вывести файлы подкаталогов, ежели нужно:
                    displayAllFilesDirectories(file);
                } else {
                  //  System.out.println("     file: " + file.getCanonicalPath()  );
                    if (getFileExtension(file).equals("json")){
                        dir_list.add(file.getCanonicalFile());
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void Find_a_Lower(String str)
    {
        boolean ok_for_body=false;
        boolean ok_for_call=false;
        if(str.indexOf("procedure_body")>0||str.indexOf("function_body")>0)
        {
            ok_for_body=true;
        }
        if(str.indexOf("procedure_call")>0||str.indexOf("function_call")>0)
        {
            ok_for_call=true;
        }
        if(ok_for_body==true&&ok_for_call==false)
        {

            System.out.println("Нижний уровень");
           // System.out.print("Имя body");

        }
        if (ok_for_body==true&&ok_for_call==true)
        {
            System.out.println("Средний уровень");
        }
    }



}