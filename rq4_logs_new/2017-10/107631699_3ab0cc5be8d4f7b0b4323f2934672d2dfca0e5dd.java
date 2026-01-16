package cn.edu.nju.cs.itrace4.core.algo;

import cn.edu.nju.cs.itrace4.core.dataset.TextDataset;
import cn.edu.nju.cs.itrace4.core.document.SimilarityMatrix;
import javafx.util.Pair;

import java.util.List;

/**
 * Created by niejia on 15/3/3.
 */
public class Basic implements CSTI {

    @Override
    public SimilarityMatrix improve(SimilarityMatrix matrix, TextDataset textDataset, SimilarityMatrix similarityMatrix) {
        return matrix;
    }

    @Override
    public SimilarityMatrix improve(SimilarityMatrix matrix, TextDataset textDataset) {
        return null;
    }

    @Override
    public String getAlgorithmName() {
        return null;
    }

    @Override
    public List<Pair<String, String>> getAlgorithmParameters() {
        return null;
    }

    @Override
    public String getDetails() {
        return null;
    }

    @Override
    public List<String> getCorrectImprovedTargetsList() {
        return null;
    }
}