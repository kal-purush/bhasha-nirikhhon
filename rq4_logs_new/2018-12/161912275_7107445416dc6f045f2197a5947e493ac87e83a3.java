package CS434;

import CS434.Exercises.Exercise;
import CS434.Muscles.Biceps;

import java.util.ArrayList;

public class BicepsDisability extends Constraint {
    BicepsDisability(Member member) {
        super(member);
        System.out.println(member + " has " + this);
    }

    @Override
    public boolean isMet() {
        ArrayList<Exercise> exercises = MainFrame.getInstance().getExerciseInvoker().getExercises();

        for (int i = 0; i < exercises.size(); i++) {
            if (exercises.get(i).getTargetMuscles().contains(Biceps.TYPE))
                return false;
        }

        return true;
    }
}