package gframework;

import java.util.ArrayList;
import java.util.List;

public class App {

    public static final int INIT_POP_SIZE = 10;
    public static final int NEW_GENERATION_SIZE = 10;
    public static final int DEFAULT_TOUR_SIZE = 5;
    public static final int NUM_GENS = 10;

    public interface loggerFunc {
        public Double get(List<Gram> in);
    }

    public static void main(String[] args) {
        Testing.loadTests(Testing.POS_SUITE_NAME, Testing.POS_TEST_SUITE_PATH);
        Testing.loadTests(Testing.NEG_SUITE_NAME, Testing.NEG_TEST_SUITE_PATH);
        GA();
    }

    public static void GA() {
        List<Gram> pop = GrammarGenerator.generatePopulation(INIT_POP_SIZE);
        List<Gram> nextGeneration = new ArrayList<>();
        for (int genNum = 0; genNum < NUM_GENS; genNum++) {

            pop.forEach(App::evaluateGram);

            for (int i = 0; i < NEW_GENERATION_SIZE; i++) {
                nextGeneration.add(Utils.tournamentSelect(pop, DEFAULT_TOUR_SIZE));
            }
            pop.clear();

            while(!nextGeneration.isEmpty()) {

                //Set these to true if you want parents to be able to survive into the next generation as well
                Gram parent1 = Utils.randGet(nextGeneration, false);
                Gram parent2 = Utils.randGet(nextGeneration, false);
                pop.addAll(Gram.crossover(parent1, parent2));

            }

            pop.forEach(Gram::mutate);

        }
    }

    public static void evaluateGram(Gram in) {

        in.injectEOF();
        Testing.generateSources(in);
        in.stripEOF();

        if (in.toRemove()) {
            System.err.println("Code gen failed for \n" + in);
            return;
        }

        try {

            int[] testResult = Testing.runTestcases(Testing.POS_SUITE_NAME, in);
            if (in.toRemove()) {
                return;
            }

            Testing.normalPositiveScoring(testResult, in);
            
            testResult = Testing.runTestcases(Testing.NEG_SUITE_NAME, in);
            
            Testing.normalNegativeScoring(testResult, in);
        } catch (Exception e) {
            System.err.println("Exception in runTests " + e.getCause());

        } finally {
            // Clears out the generated files
            Utils.deepCleanDirectory(Testing.getOutputDir(in));
        }
    }
    
}