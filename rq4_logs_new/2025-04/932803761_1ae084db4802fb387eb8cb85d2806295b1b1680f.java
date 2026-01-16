package uk.ac.bangor.cs.cambria.AcademiGymraeg;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.Noun;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.Test;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.NounRepository;

/**
 * @author dwp22pzv
 */
public class TestConfigurer {

    private NounRepository nounRepo;

    /* Gets the required number of Nouns from the repo and passes them to QuestionConfigurer. This can be a temporary solution, just doing it here to help with linking QuestionConfigurer to tests in general.
    Checks for duplicates, so should only get unique nouns.
    Once this is done, the Test can find it's associated questions by looking in the Question repo for any Questions with the matching Test object.
     * Parameters:
     *  Test test: the Test object to generate questions for. 
     */
    public void generateQuestionsForTest(Test test){

        if (test.getNumberOfQuestions() <= 0){
            throw new IllegalArgumentException("Test method .getNumberOfQuestions returned a value less than 1. This value needs to be at least 1 before questions can be generated for the test.");
        }
        
        List<Noun> allNouns = nounRepo.findAll();
        int nounCount = allNouns.size(); /*Is it more performant to count the repo directly, or should this count the list that was pulled from the repo instead, since we need to get the list anyway? */

        Random rand = new Random();
        int randomNounIndex;
        List<Integer> nounIndexList = new ArrayList<>();

        for (int i = 0; i < test.getNumberOfQuestions(); i++) {

            randomNounIndex = rand.nextInt(nounCount); /*Generate a random number between 0 and the number of nouns in the repo. */
            nounIndexList.add(randomNounIndex);

            if (test.getNumberOfQuestions() > 1) { /*Preventing multiple questions being added if this test only expects one question. */
                while (!nounIndexList.contains(randomNounIndex)){ /*Checking that the random index doesn't already exist in the list before adding it, to prevent duplicates. */
                randomNounIndex = rand.nextInt(nounCount);
                nounIndexList.add(randomNounIndex);
                }
            }
        }

        List<Noun> selectedNouns = new ArrayList<>();
        for (int nounIndex : nounIndexList){
            selectedNouns.add(allNouns.get(nounIndex)); /*nounIndex is not necessarily the same as the actual ID of the actual noun in the database table, it's just that noun's position in the list pulled from the repo. */
        }

        QuestionConfigurer questionConfig = new QuestionConfigurer();
        questionConfig.configureQuestion(selectedNouns, test);
    }





}