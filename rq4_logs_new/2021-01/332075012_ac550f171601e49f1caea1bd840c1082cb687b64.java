package by.it.petrov.jd01_07;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Matrix extends Var{

    private final double[][] value;

    public Matrix(double[][] value) {
        this.value = value;
    }

    public Matrix(Matrix matrix) {
        this.value = matrix.value;
    }

    public Matrix(String matrixStr){
        int elements = 0;
        int columns = 0;
        int rows = 0;

        Pattern patternForDoubleNumbers = Pattern.compile("[0-9.]+");
        Pattern patternForCurlyBraces = Pattern.compile("[}]");
        Matcher matcherForDoubleNumbers = patternForDoubleNumbers.matcher(matrixStr);
        Matcher matcherForCurlyBraces = patternForCurlyBraces.matcher(matrixStr);
        int indexOfFirstCurlyBraces = 0;
        if (matcherForCurlyBraces.find()){
            indexOfFirstCurlyBraces = matcherForCurlyBraces.start();
        }
        while(matcherForDoubleNumbers.find()){
            if (matcherForDoubleNumbers.start() > matcherForCurlyBraces.start()){
                elements += 1;
                continue;
            }
            columns += 1;
            elements += 1;
        }
        rows = elements / columns;
        double[][] matrix = new double[rows][columns];
        String [] stringArray = matrixStr.split("[, }]+");
        for (int i = 0; i < stringArray.length; i++) {
            stringArray[i] = stringArray[i].replace("{{", "").replace("{", "");
        }
        for (int i = 0, k = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[0].length; j++) {
                matrix[i][j] = Double.parseDouble(stringArray[k]);
                k++;
            }
        }
        this.value = matrix;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder("{{");
        for (int i = 0; i < value.length; i++) {
            for (int j = 0; j < value[0].length; j++) {
                if (j == value[0].length - 1){
                    sb = sb.append((value[i][j]));
                    if(i == value.length - 1){
                        sb = sb.append("}}");
                        break;
                    }
                    sb = sb.append("}, {");
                    continue;
                }
                sb = sb.append(value[i][j]).append(",");
            }
        }
        return sb.toString();
    }
}