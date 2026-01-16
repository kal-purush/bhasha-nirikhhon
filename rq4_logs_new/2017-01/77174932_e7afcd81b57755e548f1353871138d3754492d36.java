package janvenstermans.puzzlesolver.permutationsquare;

import janvenstermans.puzzlesolver.permutationsquare.value.IntegerPermutationSquareValue;
import janvenstermans.puzzlesolver.permutationsquare.value.PermutationSquareValueFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jan Venstermans
 */
public class PermutationSquareImplDim3Test {

    @Test
    public void test3DimOneValue() throws Exception {
        int dimension = 3;
        PermutationSquare<IntegerPermutationSquareValue> permutationSquare
                = new PermutationSquareImpl<IntegerPermutationSquareValue>(PermutationSquareValueFactory.createIntegerListForDimension(dimension));

        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> changeInfoList = new ArrayList<>();
        changeInfoList.add(new PermutationSquareCellInfo(0,0, PermutationSquareValueFactory.createIntegerListForDimension(dimension),
                PermutationSquareValueFactory.createIntegerPermutationSquareValue(1)));
        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> solvedValueList = new ArrayList<>(changeInfoList);
        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> unsolvedValueList = new ArrayList<>();
        // unsolved and all possible values
        for (int column = 0; column < dimension; column++) {
            for (int row = 1; row < dimension; row++) {
                if (column == 0 && row == 0) {
                    continue;
                }
                if (column == 0 || row == 0) {
                    unsolvedValueList.add(new PermutationSquareCellInfo(column, row,
                            PermutationSquareValueFactory.createIntegerListForAllButIntegers(dimension, 1)));
                } else {
                    unsolvedValueList.add(new PermutationSquareCellInfo(column, row,
                            PermutationSquareValueFactory.createIntegerListForDimension(dimension)));
                }
            }
        }

        permutationSquare.changeValues(changeInfoList);

        PermutationSquareLineInfoTestUtil.assertCellInfoList(permutationSquare, solvedValueList);
        PermutationSquareLineInfoTestUtil.assertCellListUnsolved(permutationSquare, unsolvedValueList);
    }

    @Test
    public void test3DimTwoValuesSameColumn() throws Exception {
        int dimension = 3;
        PermutationSquare<IntegerPermutationSquareValue> permutationSquare
                = new PermutationSquareImpl<IntegerPermutationSquareValue>(PermutationSquareValueFactory.createIntegerListForDimension(dimension));

        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> changeInfoList = new ArrayList<>();
        changeInfoList.add(new PermutationSquareCellInfo(0,0, PermutationSquareValueFactory.createIntegerListForDimension(dimension),
                PermutationSquareValueFactory.createIntegerPermutationSquareValue(1)));
        changeInfoList.add(new PermutationSquareCellInfo(1,0, PermutationSquareValueFactory.createIntegerListForDimension(dimension),
                PermutationSquareValueFactory.createIntegerPermutationSquareValue(2)));
        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> solvedValueList = new ArrayList<>(changeInfoList);
        solvedValueList.add(new PermutationSquareCellInfo(2,0, PermutationSquareValueFactory.createIntegerListForDimension(dimension),
                PermutationSquareValueFactory.createIntegerPermutationSquareValue(3)));
        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> unsolvedValueList = new ArrayList<>();
        // unsolved and all possible values
        for (int column = 0; column < dimension; column++) {
            for (int row = 1; row < dimension; row++) {
                if (column == 0) {
                    continue;
                }
                switch (row) {
                    case 0:
                        unsolvedValueList.add(new PermutationSquareCellInfo(column, row,
                                PermutationSquareValueFactory.createIntegerListForAllButIntegers(dimension, 1)));
                        break;
                    case 1:
                        unsolvedValueList.add(new PermutationSquareCellInfo(column, row,
                                PermutationSquareValueFactory.createIntegerListForAllButIntegers(dimension, 2)));
                        break;
                    case 2:
                        unsolvedValueList.add(new PermutationSquareCellInfo(column, row,
                                PermutationSquareValueFactory.createIntegerListForAllButIntegers(dimension, 3)));
                        break;
                }
            }
        }

        permutationSquare.changeValues(changeInfoList);

        PermutationSquareLineInfoTestUtil.assertCellInfoList(permutationSquare, solvedValueList);
        PermutationSquareLineInfoTestUtil.assertCellListUnsolved(permutationSquare, unsolvedValueList);
    }

    @Test
    public void test3DimTwoSameValuesDifferentColumn() throws Exception {
        int dimension = 3;
        PermutationSquare<IntegerPermutationSquareValue> permutationSquare
                = new PermutationSquareImpl<IntegerPermutationSquareValue>(PermutationSquareValueFactory.createIntegerListForDimension(dimension));

        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> changeInfoList = new ArrayList<>();
        changeInfoList.add(new PermutationSquareCellInfo(0,0, PermutationSquareValueFactory.createIntegerListForDimension(dimension),
                PermutationSquareValueFactory.createIntegerPermutationSquareValue(1)));
        changeInfoList.add(new PermutationSquareCellInfo(1,2, PermutationSquareValueFactory.createIntegerListForDimension(dimension),
                PermutationSquareValueFactory.createIntegerPermutationSquareValue(1)));
        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> solvedValueList = new ArrayList<>(changeInfoList);
        solvedValueList.add(new PermutationSquareCellInfo(2,1, PermutationSquareValueFactory.createIntegerListForDimension(dimension),
                PermutationSquareValueFactory.createIntegerPermutationSquareValue(1)));
        List<PermutationSquareCellInfo<IntegerPermutationSquareValue>> unsolvedValueList = new ArrayList<>();
        // unsolved and all values possible
        for (int column = 0; column < dimension; column++) {
            for (int row = 0; row < dimension; row++) {
                if ((row == 0 && column == 0) || (row == 1 && column == 2) || (row == 2 && column == 1)) {
                    continue;
                }
                unsolvedValueList.add(new PermutationSquareCellInfo(column, row,
                        PermutationSquareValueFactory.createIntegerListForAllButIntegers(dimension, 1)));
            }
        }

        permutationSquare.changeValues(changeInfoList);

        // all the other values should be filled in
        PermutationSquareLineInfoTestUtil.assertCellInfoList(permutationSquare, solvedValueList);
        PermutationSquareLineInfoTestUtil.assertCellListUnsolved(permutationSquare, unsolvedValueList);
    }
}