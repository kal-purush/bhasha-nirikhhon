package com.group.groupcodemarking;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JiraData implements Serializable {
    private List<Committer> jirasCompleted;
    private List<Committer> jirasCreated;


    public JiraData(List<Committer> jirasCompleted, List<Committer> jirasCreated) {
        this.jirasCompleted = jirasCompleted;
        this.jirasCreated = jirasCreated;
    }

    public List<Committer> getJirasCompleted() {
        return jirasCompleted;
    }

    public void setJirasCompleted(List<Committer> jirasCompleted) {
        this.jirasCompleted = jirasCompleted;
    }

    public List<Committer> getJirasCreated() {
        return jirasCreated;
    }

    public void setJirasCreated(List<Committer> jirasCreated) {
        this.jirasCreated = jirasCreated;
    }




    public List<Integer> getTeamScores() {
        List<Integer> values = new ArrayList<>();
        jirasCompleted.forEach(e -> {
            values.add(e.getValue() + jirasCreated.stream().filter(f -> f.getName().equals(e.getName())).findFirst().get().getValue());
        });

        double sum = values.stream()
                .mapToDouble(a -> a)
                .sum();

        if (sum > 200) {
            return  MathHelper.normaliseList(values, 60, 100);
        } else if (sum > 150) {
            return  MathHelper.normaliseList(values, 50, 90);
        } else if (sum > 125) {
            return  MathHelper.normaliseList(values, 40, 80);
        }  else if (sum > 100) {
            return  MathHelper.normaliseList(values, 30, 70);
        }  else if (sum > 80) {
            return   MathHelper.normaliseList(values, 20, 60);
        }  else if (sum > 50) {
            return  MathHelper.normaliseList(values, 10, 50);
        } else if (sum > 35) {
            return  MathHelper.normaliseList(values, 10, 40);
        } else {
            return  MathHelper.normaliseList(values, 1, 35);
        }
    }
}