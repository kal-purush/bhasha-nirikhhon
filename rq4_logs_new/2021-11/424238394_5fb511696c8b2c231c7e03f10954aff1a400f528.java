/*
 *@author Tevfik Kesici
 *@since 02.10.2021
 */

import java.util.ArrayList;
import java.util.Scanner;

public class Puzzle {
    public static void main(String[] args) throws Exception {
        Create create = new Create();
        create.createFirst();
        create.createSecond();
        create.createOutput();
        Solve solve = new Solve
                (create.getFirst(), create.getSecond(), create.getOutput());
        System.out.println(solve.getFirstLetters() + "-" +
                solve.getSecondLetters() + "-" + solve.getOutputLetters());
        System.out.println("All letters: " + solve.getAllLetters());
        solve.Search(create.getFirst(), create.getSecond(), create.getOutput());


    }
}

class Create {
    Scanner sc = new Scanner(System.in);

    private String first;
    private String second;
    private String output;

    public String createFirst() throws Exception {
        System.out.print("Enter 1st input value: ");
        first = sc.nextLine();
        if (!(first.length() >= 2 && first.length() <= 6)) {
            throw new IllegalArgumentException
                    ("The length of input must be greater than or equal " +
                            "to 2 or less than or equal to 6.");
        }
        return first;
    }

    public String createSecond() throws Exception {
        System.out.print("Enter 2nd input value: ");
        second = sc.nextLine();
        if (!(second.length() >= 2 && second.length() <= 6)) {
            throw new IllegalArgumentException
                    ("The length of input must be greater than or equal " +
                            "to 2 or less than or equal to 6.");
        }
        return second;
    }

    public String createOutput() throws Exception {
        System.out.print("Enter Output value:    ");
        output = sc.nextLine();
        return output;
    }

    public String getFirst() {
        return this.first;
    }

    public String getSecond() {
        return this.second;
    }

    public String getOutput() {
        return this.output;
    }

}

class Solve {
    Scanner sc = new Scanner(System.in);
    private ArrayList firstLetters = new ArrayList();
    private ArrayList secondLetters = new ArrayList();
    private ArrayList outputLetters = new ArrayList();
    private ArrayList allLetters = new ArrayList();
    private ArrayList<Integer> values = new ArrayList();

    public Solve(String first, String second, String output) {
        for (int i = 0; i < first.length(); i++) {
            if (!(firstLetters.contains(first.charAt(i)))) {
                firstLetters.add(first.charAt(i));
            }
            if (!(allLetters.contains(first.charAt(i)))) {
                allLetters.add(first.charAt(i));
            }
        }
        for (int i = 0; i < second.length(); i++) {
            if (!(secondLetters.contains(second.charAt(i)))) {
                secondLetters.add(second.charAt(i));
            }
            if (!(allLetters.contains(second.charAt(i)))) {
                allLetters.add(second.charAt(i));
            }
        }
        for (int i = 0; i < output.length(); i++) {
            if (!(outputLetters.contains(output.charAt(i)))) {
                outputLetters.add(output.charAt(i));
            }
            if (!(allLetters.contains(output.charAt(i)))) {
                allLetters.add(output.charAt(i));
            }

        }


    }

    public void Map(ArrayList allLetters) {
        values.clear();
        int j;
        for (int i = 0; i < allLetters.size(); i++) {
            j = (int) (Math.random() * 10);
            values.add(j);

        }
    }

    public boolean Search(String first, String second, String Output) {
        System.out.print("searching...");
        boolean found = false;
        if (!found) {
            Map(allLetters);
        }
        int firstSum = 0;
        int secondSum = 0;
        int Result = 0;
        System.out.println(values);
        for (int i = first.length() - 1; i >= 0; i--) {
            if (allLetters.contains(first.charAt(i))) {
                firstSum = firstSum + values.get(first.length() - i - 1) *
                        (int) Math.pow(10, i);
            }
            System.out.println(firstSum);
        }
        for (int i = second.length() - 1; i >= 0; i--) {
            if (allLetters.contains(second.charAt(i))) {
                secondSum = secondSum + values.get(second.length() - i - 1) *
                        (int) Math.pow(10, i);
            }
            System.out.println(secondSum);
        }
        for (int i = Output.length() - 1; i >= 0; i--) {
            if (allLetters.contains(Output.charAt(i))) {
                Result = Result + values.get(Output.length() - i - 1) *
                        (int) Math.pow(10, i);
            }
            System.out.println(Result);
        }
        if (firstSum + secondSum == Result) {
            found = true;
            System.out.print("found!");
        } else {
            Map(allLetters);
            System.out.println("trying again");
        }
        return found;
    }

    public ArrayList getFirstLetters() {

        return this.firstLetters;
    }

    public ArrayList getSecondLetters() {
        return this.secondLetters;
    }

    public ArrayList getOutputLetters() {
        return this.outputLetters;
    }

    public ArrayList getAllLetters() {
        return this.allLetters;
    }

    public ArrayList getValues() {
        return this.values;
    }
}