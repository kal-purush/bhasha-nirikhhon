package org.monarchinitiative.vmvt.core.jaspar;

import org.monarchinitiative.vmvt.core.except.VmvtException;
import org.monarchinitiative.vmvt.core.except.VmvtRuntimeException;
import org.monarchinitiative.vmvt.core.pssm.DoubleMatrix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JasparMatrix {

    private final String jasparId;
    private final String jasparName;
    //private final DoubleMatrix frequencyMatrix;

    JasparMatrix(String nameLine, String Aline, String Cline, String Gline, String Tline){
        if (nameLine ==null || nameLine.isEmpty()) {
            throw new VmvtRuntimeException("Malformed JASPAR name line (empty/null)");
        } else if (!nameLine.startsWith(">")) {
            throw new VmvtRuntimeException("Malformed JASPAR name line (did not start with \'>\'): " + nameLine);
        }
        // remove the '>' character and split into two fields
        String [] fields = nameLine.substring(1).split("\t");
        if (fields.length != 2) {
            throw new VmvtRuntimeException("Malformed JASPAR name line (did not start with \'>\'): " + nameLine);
        }
        jasparId = fields[0];
        jasparName = fields[1];
        if (! Aline.startsWith("A")) {
            throw new VmvtRuntimeException("Malformed JASPAR A line: " + Aline);
        } else if (! Cline.startsWith("C")) {
            throw new VmvtRuntimeException("Malformed JASPAR C line: " + Cline);
        } else if (! Gline.startsWith("G")) {
            throw new VmvtRuntimeException("Malformed JASPAR G line: " + Gline);
        } else if (! Tline.startsWith("T")) {
            throw new VmvtRuntimeException("Malformed JASPAR T line: " + Tline);
        }
        List<Double> A = getCountLine(Aline);
        List<Double> C = getCountLine(Cline);
        List<Double> G = getCountLine(Gline);
        List<Double> T = getCountLine(Tline);
        int N = A.size();
        if (N != C.size()) {
            throw new VmvtRuntimeException("Malformed JASPAR C array (Counts do not match A): ");
        } else  if (N != G.size()) {
            throw new VmvtRuntimeException("Malformed JASPAR G array (Counts do not match A): ");
        } else  if (N != T.size()) {
            throw new VmvtRuntimeException("Malformed JASPAR T array (Counts do not match A): ");
        }
        for (int i=0;i<N;i++) {
            double sum = A.get(i) + C.get(i) + G.get(i) + T.get(i);
            A.set(i, A.get(i) / sum);
            C.set(i, C.get(i) / sum);
            G.set(i, G.get(i) / sum);
            T.set(i, T.get(i) / sum);
        }
        final List<List<Double>> donor
                = List.of(A,C,G,T);

    }



    private List<Double> getCountLine(String line) {
        int i = line.indexOf("[");
        if (i<0) {
            throw new VmvtRuntimeException("Malformed JASPAR name line (no [): " + line);
        }
        int j = line.indexOf("]");
        if (j<0 || j < i) {
            throw new VmvtRuntimeException("Malformed JASPAR name line (no ]): " + line);
        }
        line = line.substring(i+1,j).strip();
        String [] fields = line.split("\t");
        return Arrays.stream(fields).map(Double::parseDouble).collect(Collectors.toList());
    }



}