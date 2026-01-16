/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Kmeans;

import java.util.ArrayList;

/**
 *
 * @author Mads
 */
public class Centroid {
    public ArrayList<Text> GroupedDocument;

    public ArrayList<Text> getGroupedDocument() {
        return GroupedDocument;
    }

    public void setGroupedDocument(ArrayList<Text> GroupedDocument) {
        this.GroupedDocument = GroupedDocument;
    }
}