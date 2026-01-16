//**********************************************************************
// Copyright (c) 2017 Telefonaktiebolaget LM Ericsson, Sweden.
// All rights reserved.
// The Copyright to the computer program(s) herein is the property of
// Telefonaktiebolaget LM Ericsson, Sweden.
// The program(s) may be used and/or copied with the written permission
// from Telefonaktiebolaget LM Ericsson or in accordance with the terms
// and conditions stipulated in the agreement/contract under which the
// program(s) have been supplied.
// **********************************************************************
package io.github.marcelovca90.helper;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import smile.data.Attribute;
import smile.data.AttributeDataset;
import smile.data.NumericAttribute;
import smile.feature.SignalNoiseRatio;
import smile.feature.SumSquaresRatio;

public class FeatureSelectionHelper
{
    public static AttributeDataset signalNoiseRatio(AttributeDataset dataset)
    {
        double[][] x = dataset.toArray(new double[dataset.size()][]);
        int[] y = dataset.toArray(new int[dataset.size()]);

        SignalNoiseRatio snr = new SignalNoiseRatio();
        double[] rankSnr = snr.rank(x, y);

        return reduce(dataset, rankSnr);
    }

    public static AttributeDataset sumSquaresRatio(AttributeDataset dataset)
    {
        double[][] x = dataset.toArray(new double[dataset.size()][]);
        int[] y = dataset.toArray(new int[dataset.size()]);

        SumSquaresRatio ssr = new SumSquaresRatio();
        double[] rankSsr = ssr.rank(x, y);

        return reduce(dataset, rankSsr);
    }

    private static AttributeDataset reduce(AttributeDataset dataset, double[] ranks)
    {
        // retrieve relevant attributes indices
        List<Integer> relevantAtts = new LinkedList<>();
        for (int i = 0; i < dataset.attributes().length; i++)
            if (!Double.isNaN(ranks[i]))
                relevantAtts.add(i);

        // build the attributes for the reduced data set
        List<Attribute> atts = relevantAtts.stream().map(i -> new NumericAttribute("x" + i)).collect(Collectors.toList());

        // copy only the relevant attributes from the original data set
        AttributeDataset reducedDataSet = new AttributeDataset("reducedDataSet", atts.toArray(new Attribute[0]), dataset.response());
        for (int i = 0; i < dataset.size(); i++)
        {
            double[] x = new double[relevantAtts.size()];
            for (int j = 0; j < relevantAtts.size(); j++)
                x[j] = dataset.get(i).x[relevantAtts.get(j)];
            int y = (int) dataset.get(i).y;
            reducedDataSet.add(x, y);
        }

        return reducedDataSet;
    }
}