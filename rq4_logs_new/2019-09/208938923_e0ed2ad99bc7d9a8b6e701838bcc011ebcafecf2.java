package com.sovannarith;

import com.sovannarith.work1.currency$converter.CurrencyConverter;
import com.sovannarith.work1.currency$converter.Riel;
import com.sovannarith.work1.currency$converter.Usd;
import com.sovannarith.work1.read$file.MultiChoiceQA;
import com.sovannarith.work1.read$file.QuestionForm;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws IOException {
        currencyConverter();
        javaSwing();
    }

    public static void javaSwing() throws IOException {
        List<MultiChoiceQA> multiChoiceQAS = readFile("qa.json");
        QuestionForm form = null;
        for (MultiChoiceQA qa : multiChoiceQAS) {
            form = new QuestionForm(qa);
            form.setBounds(200, 200, 600, 400);
            form.setTitle("RadioButtons");
            form.setVisible(true);
        }
    }
    public static void currencyConverter() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("1 for USD to Riel");
        System.out.println("2 for Riel to USD");
        String option = scanner.nextLine();
        boolean bool = option == null || !option.matches("1|2");
        if (bool) {
            System.out.println("option is not in the list");
            do {
                option = scanner.nextLine();
            } while (!bool);
        }
        CurrencyConverter converter = option.equals("1") ? new Usd() : new Riel();

        System.out.println("Amount:");
        converter.setAmount(scanner.nextDouble());

        System.out.println("Exchange Rate:");
        converter.setExchangeRate(scanner.nextDouble());

        converter.printOutput();
        scanner.close();
    }


    private static String readFile(File file) throws IOException {
        StringBuilder sb = new StringBuilder();
        InputStream in = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line + System.lineSeparator());
        }

        return sb.toString();
    }
    public static List<MultiChoiceQA> readFile(String fileName) throws IOException {
        String json = readFile(new File(fileName));
        List<MultiChoiceQA> list = new ArrayList<>();
        JSONArray questions = new JSONObject(json).getJSONArray("questions");
        for (Object question : questions) {
            List<String> ans = new ArrayList<>();
            JSONObject qe = new JSONObject(question.toString());
            MultiChoiceQA multiChoiceQA = new MultiChoiceQA();
            for (Iterator<String> it = qe.keys(); it.hasNext(); ) {
                String o = it.next();
                if (o.matches("q")) multiChoiceQA.setQuestion(qe.getString(o));
                else if (o.matches("ca")) multiChoiceQA.setCorrectAnswer(qe.getString(qe.getString(o)));
                else {
                    ans.add(qe.getString(o));
                }
            }
            multiChoiceQA.setAnswers(ans);
            list.add(multiChoiceQA);
        }
        return list;
    }

}