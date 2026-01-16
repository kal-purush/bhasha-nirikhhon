package org.wecancodeit.backend.BOService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;
import org.wecancodeit.backend.DataModels.AnimalQuestionModel;
import org.wecancodeit.backend.Enums.DifficultyLevel;

import jakarta.annotation.Resource;

@Service
public class AnimalQuestionService {

    private List<AnimalQuestionModel> animalQuestions;

    public AnimalQuestionService() {
        animalQuestions = new ArrayList<>(); //Creating an arraylist with the hardcoded questions

        animalQuestions.add(new AnimalQuestionModel(
                (long) 1,
                "What is the largest mammal?",
                Arrays.asList("Elephant", "Blue Whale", "Giraffe", "Hippopotamus"),
                "Blue Whale",
                "https://www.oceanactionhub.org/storage/2024/08/A-blue-whales-heart-dwarfs-a-small-car-in-size-1024x702.jpg",
                DifficultyLevel.Beginner

        ));

        animalQuestions.add(new AnimalQuestionModel(
                (long) 2,
                "Which animal is known for having a pouch?",
                Arrays.asList("Penguin", "Kangaroo", "Elephant", "Panda"),
                "Kangaroo",
                "https://cdn.mos.cms.futurecdn.net/ETb2xLjvc62eb7PPLspSsU.jpg",
                DifficultyLevel.Beginner

        ));

        animalQuestions.add(new AnimalQuestionModel(
                (long) 3,
                "What animal is known for making honey?",
                Arrays.asList("Bee", "Wasp", "Ant", "Mongoose"),
                "Bee",
                "https://www.utm.utoronto.ca/hospitality/sites/files/hospitality/styles/landscape/public/2024-10/Worker%20Bee.jpg.webp?itok=ZN9njD6r",
                DifficultyLevel.Beginner

        ));

        animalQuestions.add(new AnimalQuestionModel(
                (long) 4,
                "How many legs does a spider have?",
                Arrays.asList("6", "8", "10", "12"),
                "8",
                "https://i.natgeofe.com/n/76aea853-00cf-41f4-a618-3a1b0ff2afb4/spiders_07_3x4.jpg",
                DifficultyLevel.Intermediate));

        animalQuestions.add(new AnimalQuestionModel(
                (long) 5,
                "What is a group of lions called?",
                Arrays.asList("Herd", "Pack", "Flock", "Pride"),
                "Pride",
                "https://cdn.mos.cms.futurecdn.net/FVqUjfbiHS9imyJiRiM53-1200-80.jpg",
                DifficultyLevel.Intermediate));

        animalQuestions.add(new AnimalQuestionModel(
                (long) 6,
                "What is the fastest land animal?",
                Arrays.asList("Cheetah", "Horse", "Lion", "Elephant"),
                "Cheetah",
                "https://seethewild.org/wp-content/uploads/2022/09/Cheetah_Kruger-1024x671-1.jpg",
                DifficultyLevel.Intermediate));

        animalQuestions.add(new AnimalQuestionModel(
                (long) 7,
                "Which bird is known for its ability to mimic sounds?",
                Arrays.asList("Parrot", "Sparrow", "Eagle", "Owl"),
                "Parrot",
                "https://t4.ftcdn.net/jpg/07/18/12/87/360_F_718128776_nJReWqPkf5qF4Y5na8ZqGWAbdCJTpczZ.jpg",
                DifficultyLevel.Advanced));

        animalQuestions.add(new AnimalQuestionModel(
                (long) 8,
                "What animal is used to hunt for mushrooms",
                Arrays.asList("Pig", "Rat", "Cat", "Python"),
                "Parrot",
                "https://i.natgeofe.com/k/0ed36c42-672a-425b-9e62-7cc946b98051/pig-fence_square.jpg",
                DifficultyLevel.Advanced));

        animalQuestions.add(new AnimalQuestionModel(
                (long) 9,
                "Which animal is used to produce designer fabric",
                Arrays.asList("Inchworm", "Earworm", "Silkworm", "Ringworm"),
                "Silkworm",
                "https://news.cgtn.com/news/3d3d514f7751544d33457a6333566d54/img/09d34ce2ac6c465cb09306d27a2d9083/09d34ce2ac6c465cb09306d27a2d9083.jpg",
                DifficultyLevel.Advanced));
    }

    /*
     * Method to return all the animal questions
     */
    public List<AnimalQuestionModel> getAllAnimalQuestions(){
        return animalQuestions;
    }






    // // Random instance used to generate random ages for pets
    // private final Random randomBeginner = new Random(); // used to generate
    // random values within a range and difficulty

    // //Minimum and maximum values associated with each difficulty
    // public ArrayList<AnimalQuestionModel> generateListOfQuestion(DifficultyLevel
    // difficultyLevel){
    // ArrayList<AnimalQuestionModel> result = new ArrayList<>();

    // switch (difficultyLevel){

    // case Beginner: {

    // }

    // case Intermediate: {

    // }

    // case Advanced: {

    // }
    // }
    // return result;
    // }

    // private ArrayList<AnimalQuestionModel> generateBeginnerQuestion(){
    // ArrayList<
    // }

}