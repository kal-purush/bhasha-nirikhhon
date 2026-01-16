import java.util.*;
import java.io.*;

public class TestExisting
{
    public static void main(String[] args) throws IOException
    {
        String[] lang = {"bul", "ces", "deu", "fin", "slk", "spa", "pes", "tur"};

        PrintStream out = new PrintStream("exiting-results.txt");
        PrintStream exp = new PrintStream("example-mistakes.txt");
        for (String l1 : lang)
        {
            for (String l2 : lang) if (!l1.equals(l2))
            {
                String comb = l1 + "-" + l2;
                System.out.println(comb);

                compare(l1, l2, false, out, exp);
            }
        }
        out.close();
        exp.close();
    }

    public static void compare(String l1, String l2, boolean fine, PrintStream out, PrintStream exp) throws IOException
    {
        Scanner ins = new Scanner(new File("../scrapping/files/"+l1 + ".txt"), "UTF8");
        Scanner ino = new Scanner(new File("../scrapping/files/"+l2 + ".txt"), "UTF8");
        Scanner inp = new Scanner(new File("../aligning/output/"+l1 + "-" + l2 + (fine ? "-fine.txt" : ".txt")), "UTF8");

        int correct = 0;
        int total = 0;
        double preds = 0;
        ArrayList<String> mistakes = new ArrayList<>();
        while (inp.hasNextLine() && ino.hasNextLine() && ins.hasNextLine())
        {
            String line1 = ino.nextLine();
            String line2 = inp.nextLine();
            String line3 = ins.nextLine();
            if (line1.length()==0 || line1.equals("----------") || line1.startsWith("/study/scriptures"))
                continue;

            ArrayList<Integer> tag1 = getTaggedWords(line1);

            if (tag1.size() > 0)
            {
                boolean mistake = false;
                int countSource = getTaggedWords(line3).size();
                ArrayList<Integer> tag2 = getTaggedWords(line2);

                total += Math.min(countSource, tag1.size());

                if (countSource <= tag1.size())
                {
                    HashSet<Integer> tag1Set = new HashSet<>();
                    tag1Set.addAll(tag1);

                    preds += tag2.size();

                    for (int num : tag2)
                    {
                        if (tag1Set.contains(num)) ++correct;
                        else mistake = true;
                    }
                }
                else
                {
                    HashSet<Integer> tag2Set = new HashSet<>();
                    tag2Set.addAll(tag2);

                    preds += (double)tag2.size() * tag1.size() / countSource;

                    for (int num : tag1)
                    {
                        if (tag2Set.contains(num)) ++correct;
                        else mistake = true;
                    }
                }

                if (mistake) mistakes.add(line3 + "\n" + line2 + "\n" + line1);
            }
        }

        if (mistakes.size()>0)
        {
            exp.println(l1 + "->" + l2);
            exp.println("-------");
            for (int i=0;i<5 && i<mistakes.size();++i)
            {
                //TODO: SWAP
                exp.println(mistakes.get(i));
                exp.println();
            }
            exp.println();
        }

        ins.close();
        ino.close();
        inp.close();

        if (total == 0 || preds == 0)
        {
            return;
        }

        out.println(l1 + "->" + l2);
        out.println(correct);
        out.println(total);
        out.println((int)preds);
        
        double precision = 100.0 * correct / total;
        double recall = 100.0 * correct / preds;
        double f1 = 2 / (1 / precision + 1 / recall);

        out.printf("%.3f\n", precision);
        out.printf("%.3f\n", recall);
        out.printf("%.3f\n", f1);
        out.println();
    }

    public static ArrayList<Integer> getTaggedWords(String line)
    {
        ArrayList<Integer> answer = new ArrayList<>();

        int state = 0; //0 - out of word, 1 - in word, 2 - in puct
        int wordCount = 0;

        for (char c : line.toCharArray())
        {
            if (Character.isWhitespace(c) || c == 0x00A0)
            {
                state = 0;
            }
            else if (c == '~')
            {
                answer.add(wordCount);
            }
            else if (Character.isAlphabetic(c) || c==0x2019)
            {
                if (state!=1)
                {
                    ++wordCount;
                }
                state = 1;
            }
            else
            {
                if (state!=2)
                {
                    ++wordCount;
                }
                state = 2;
            }
        }

        return answer;
    }
}