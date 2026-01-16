/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Kmeans;

import analyzertfidf.Text;
import java.util.ArrayList;

/**
 *
 * @author Mads
 */
public class Comparer {
    public Centroid compareText(ArrayList<Centroid> corpus, Text t) {
        Clustering clusterText = new Clustering();
        ArrayList<Centroid> nonEmptyClusters = new ArrayList<>();
        //Only look though clusters that are not empty
        for(Centroid c : corpus) {
            if(!c.GroupedDocument.isEmpty()) {
                nonEmptyClusters.add(c);
            }
        }
        int index = clusterText.FindClosestClusterCenter(nonEmptyClusters, t);
        return corpus.get(index);
    }
}