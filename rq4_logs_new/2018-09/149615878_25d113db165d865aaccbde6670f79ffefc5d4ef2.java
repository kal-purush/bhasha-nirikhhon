import java.awt.*;
import java.io.*;
import java.util.*;

public class LibraryManagement {
    public void showAllWords() {
        for (Map.Entry<String, String> entry : Dictionary._library.entrySet()) { //Cho iterator chay tu dau den cuoi map, k dung dc for
            System.out.println("Word: " + entry.getKey() + " Meaning: " + entry.getValue());
            System.out.println("-----------------------------------------------------------------------");
        }
    }

    private void insertFromUser() {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter number of words: ");
        int n = sc.nextInt();
        sc.nextLine();
        for (int i = 0; i < n; i++) {
            System.out.println("Enter the word: ");
            String word = sc.nextLine();
            System.out.println("Enter the meaning: ");
            String meaning = sc.nextLine();
            if (!Dictionary._library.containsKey(word)) Dictionary._library.put(word, meaning);
            else {
                System.out.println("Existed word");
                i--; // Try again, thu dien` lai vi user dien vao tu da co trong tu dien
            }
        }
    }

    private void insertFromFile() {
        Scanner scanner = null;
        String filename_path = "src\\library.txt";
        try{
            scanner = new Scanner(new File(filename_path));

        }catch (FileNotFoundException e) {
            scanner = new Scanner(System.in);
            System.out.println("File not found");
        }

        while (scanner.hasNext()) {
            String word_target = scanner.next();
            String word_meaning = scanner.nextLine();
            Dictionary._library.put(word_target, word_meaning);
        }
    }

    private void exportToFile(){
        String path = "src\\library.txt";
        for (Map.Entry<String, String> entry : Dictionary._library.entrySet()) {
            try (FileWriter fw = new FileWriter(path, true);
                 BufferedWriter bf = new BufferedWriter(fw);
                 PrintWriter pw = new PrintWriter(bf)) {
                pw.println(entry.getKey() + " " + entry.getValue());
            } catch (Exception e) {

            }
        }
    }

    public static void loadProgram() { //Khi bat dau chuong trinh se chay ham nay de load thu vien va hien tat ca ra
        LibraryManagement libraryManagement = new LibraryManagement();
        libraryManagement.insertFromFile();
        //libraryManagement.showAllWords();
    }

    private void userAdd() { //Them tu vao tu dien va file
        LibraryManagement libraryManagement = new LibraryManagement();
        libraryManagement.insertFromUser();
        libraryManagement.exportToFile();
    }

    private void userModify() {
        LibraryManagement libraryManagement = new LibraryManagement();
        System.out.println("Choose your option: ");
        System.out.printf("1) Modify the word \n2) Modify the meaning \n3) Modify both its word and meaning \n");
        Scanner sc = new Scanner(System.in);
        int choose = sc.nextInt();
        sc.nextLine();
        if (choose == 1) { //Sua tu
            System.out.println("Choose the word to modify: ");
            String word_target = sc.nextLine();

            if (Dictionary._library.containsKey(word_target)) {
                System.out.println("Type in your word modification: ");
                String wordModify = sc.nextLine();

                //Sua word
                String meaning = Dictionary._library.get(word_target);
                Dictionary._library.remove(word_target);
                Dictionary._library.put(wordModify, meaning);

                // Save file
                libraryManagement.exportToFile();
                System.out.println("Success! Word after modification: " + wordModify + " | Meaning after modification: " + meaning);

            }
            else System.out.println("Word not found");
        }
        else if (choose == 2) { //Sua nghia
            System.out.println("Choose the word to modify: ");
            String word_target = sc.nextLine();

            if (Dictionary._library.containsKey(word_target)) {
                System.out.println("Type in your meaning modification: ");
                String meaningModify = sc.nextLine();

                Dictionary._library.replace(word_target, meaningModify);

                libraryManagement.exportToFile();
                System.out.println("Success! Word after modification: " + word_target + " | Meaning after modification: " + meaningModify);
            }
            else System.out.println("Word not found");
        }
        else { //Sua ca tu va nghia
            System.out.println("Choose the word to modify: ");
            String word_target = sc.nextLine();

            if (Dictionary._library.containsKey(word_target)) {
                System.out.println("Type in your word modification: ");
                String wordModify = sc.nextLine();

                System.out.println("Type in your meaning modification: ");
                String meaningModify = sc.nextLine();

                Dictionary._library.remove(word_target);
                Dictionary._library.put(wordModify, meaningModify);

                libraryManagement.exportToFile();
                System.out.println("Success! Word after modification: " + wordModify + " | Meaning after modification: " + meaningModify);
            }
            else System.out.println("Word not found");
        }
    }

    private void userDelete() {
        LibraryManagement libraryManagement = new LibraryManagement();
        Scanner sc = new Scanner(System.in);
        System.out.println("Choose the word to delete: ");
        String word_target = sc.nextLine();
        if (Dictionary._library.containsKey(word_target)) {
            Dictionary._library.remove(word_target);

            libraryManagement.exportToFile();
            System.out.println("Success!");
        }
        else System.out.println("Word not found");
    }

    private void wordSearch() {
        System.out.println("Searching for: ");
        Scanner sc = new Scanner(System.in);
        String word_target = sc.nextLine();
        if (Dictionary._library.containsKey(word_target)) {
            System.out.println(word_target + " meaning: " + Dictionary._library.get(word_target));
        }
//        else {
//            //Thuat toan tim kiem (de sau)
//        }
    }

    public void tools() {
        boolean exit = false;
        LibraryManagement libraryManagement = new LibraryManagement();
        while(!exit) {
            System.out.println("|-----------------------------------------------------------------------------------------|");
            System.out.print("|OPTIONS:\n|1) Search for a word\n|2) Modify a word\n|3) Delete a word\n|4) Add more words\n|0) Close App\n");
            System.out.println("|Choose your option: ");
            System.out.println("|-----------------------------------------------------------------------------------------|");
            Scanner sc = new Scanner(System.in);
            String option = sc.nextLine();

            if (option.equals("0")) exit = true;

            else if (option.equals("1")) libraryManagement.wordSearch();

            else if (option.equals("2")) libraryManagement.userModify();

            else if (option.equals("3")) libraryManagement.userDelete();

            else if (option.equals("4")) libraryManagement.userAdd();
        }
    }
}