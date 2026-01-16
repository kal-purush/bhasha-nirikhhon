package viewer;

import Controler.FileOperations;
import Controler.HashTable;
import Logics.SearchAlgorithem;

public class SearchEngineApp {

    static HashTable Document1;
    static HashTable Document2;
    static HashTable Document3;
    static FileOperations file1;
    static FileOperations file2;
    static FileOperations file3;
    static String Query;
    static SearchAlgorithem sa = new SearchAlgorithem();

    public static void main(String args[]) {

       
        file1 = new FileOperations("C:\\Users\\acer\\Documents\\NetBeansProjects\\Search_Engine\\src\\Document1.txt");
        file2 = new FileOperations("C:\\Users\\acer\\Documents\\NetBeansProjects\\Search_Engine\\src\\Document2.txt");
        file3 = new FileOperations("C:\\Users\\acer\\Documents\\NetBeansProjects\\Search_Engine\\src\\Document3.txt");

        Document1 = new HashTable(file1.wordcount());
        Document1.setName("Document 1");
        file1.fillData(Document1);
        Document1.dispalyTable();
        System.out.println();

        Document2 = new HashTable(file2.wordcount());
        Document2.setName("Document 2");
        file2.fillData(Document2);
        Document2.dispalyTable();
        System.out.println();

        Document3 = new HashTable(file3.wordcount());
        Document3.setName("Document 3");
        file3.fillData(Document3);
        Document3.dispalyTable();
        System.out.println();

        Query = "open and university";

        sa.find(Query,Document1);
        sa.find(Query, Document2);
        sa.find(Query, Document3);
    }
}