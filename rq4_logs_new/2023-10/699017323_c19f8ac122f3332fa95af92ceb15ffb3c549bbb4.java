package collections;

import java.util.ArrayList;

import java.util.*;

import core.Tech;
import learner.AgeComparator;
import learner.IdComparator;
import learner.Learner;

public class ListExample {
    public static void main(String[] args) {
        arrayLists();
    }

    public static void arrayLists(){
        ArrayList<Learner> learners = new ArrayList<>();
        
        Learner fLearner = new Learner("First", 19 , Tech.Python);
        Learner sLearner = new Learner("Second", 25 , Tech.Java);
        Learner tLearner = new Learner("Third", 17 , Tech.Unity);
        Learner tcLearner = new Learner("Third", 17 , Tech.Unity);

        learners.add(fLearner);
        learners.add(sLearner);
        learners.add(tLearner);
        learners.add(tcLearner);

        learners.add(new Learner("Artem", 19, Tech.Android));

        System.out.println("Learners size: " + learners.size());

        System.out.println("Second contains? " + learners.contains(sLearner));

        int tLearnerIndex = learners.indexOf(tLearner);
        System.out.println("Index of third learner: " + tLearnerIndex);

        Learner copyLearner = learners.get(1);
        System.out.println("Copy learner (get): " + copyLearner);

        System.out.println("");

        Iterator iterator = learners.iterator();
        while(iterator.hasNext()){
            Learner itLearner = (Learner) iterator.next();
            System.out.println("Learners: " + itLearner);
        }

        Collections.sort(learners, new AgeComparator());
        
        System.out.println("");
        System.out.println("Sort by age");
        
        Iterator iterator2 = learners.iterator();
        while(iterator2.hasNext()){
            Learner itLearner = (Learner) iterator2.next();
            System.out.println("Learners: " + itLearner);
        }

        Collections.sort(learners, new IdComparator());
        
        System.out.println("");
        System.out.println("Sort by id");
        
        Iterator iterator3 = learners.iterator();
        while(iterator3.hasNext()){
            Learner itLearner = (Learner) iterator3.next();
            System.out.println("Learners: " + itLearner);
        }
        
    }

}