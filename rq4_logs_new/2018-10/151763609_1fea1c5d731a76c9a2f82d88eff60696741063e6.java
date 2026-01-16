import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DanceBattle {

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        final int testCases = Integer.parseInt(br.readLine());

        for (int testCase = 1; testCase <= testCases; testCase++) {

            int ourHonor = 0;
            int ourEnergy = Integer.parseInt(br.readLine().split(" ")[0]);

            // Sort the array for easy lookup of min & max
            // We can order the teams any way we want because we have the option to delay any team without a limit
            LinkedList<Integer> rivalTeams = Arrays.stream(br.readLine().split(Pattern.quote(" ")))
                    .mapToInt(Integer::parseInt).sorted().boxed().collect(Collectors.toCollection(LinkedList::new));

            // The idea is that we want to recruit the highest skill and gain honor by dancing against the lowest skill
            while (!rivalTeams.isEmpty()) {

                // Keep dancing off with low skilled rival teams until
                // we run out of energy or there are no more teams left
                while (!rivalTeams.isEmpty() && ourEnergy > rivalTeams.getFirst()) {

                    //System.out.println("Dance off: " + ourEnergy + " vs. " + rivalTeams.getFirst());

                    ourHonor++;
                    ourEnergy -= rivalTeams.removeFirst();

                    //System.out.println("Our honor: " + ourHonor + " | Our energy: " + ourEnergy);
                    //System.out.println("Teams left: " + rivalTeams.size());

                }

                // Check to see if there are any other teams left
                if (rivalTeams.isEmpty())
                    break;

                // If our honor is 0 after the first loop, this means that we did not find any teams
                // that we can dance against to increase our honor. Therefore, truce with all the others.
                if (ourHonor == 0)
                    break;

                // If there is only 1 team left, and we can't beat them, don't bother recruiting them (truce)
                if (rivalTeams.size() == 1)
                    break;

                //System.out.println("Recruit with " + rivalTeams.getLast());

                // Recruit a the team with the highest skill to continue the dance off against lower skilled teams
                ourEnergy += rivalTeams.removeLast();
                ourHonor--;

                //System.out.println("Our honor: " + ourHonor + " | Our energy: " + ourEnergy);
                //System.out.println("Teams left: " + rivalTeams.size());

                // Repeat

            }

            System.out.println("Case #" + testCase + ": " + ourHonor);

        }

        br.close();

    }

}