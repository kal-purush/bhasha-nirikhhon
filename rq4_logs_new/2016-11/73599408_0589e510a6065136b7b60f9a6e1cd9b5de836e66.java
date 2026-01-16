package com.example.jason.linearalgebraapp.activities.Fragments;

import android.graphics.Color;
import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.text.InputType;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.Spinner;
import android.widget.TextView;

import com.example.jason.linearalgebraapp.R;

/**
 * Created by jason on 11/13/2016.
 */

public class TransposeMatrix extends Fragment {
    int numberOfRows;
    int numberOfColumns;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {

        View rootView = inflater.inflate(R.layout.fragment_transposematrix, container, false);

        final LinearLayout matrix = (LinearLayout) rootView.findViewById(R.id.matrix);
        final LinearLayout spinners = (LinearLayout) rootView.findViewById(R.id.spinners);
        final LinearLayout result = (LinearLayout) rootView.findViewById(R.id.result);
        Spinner rowsSpinner = new Spinner(getContext());
        Spinner columnSpinner = new Spinner(getContext());
        Button transposeButton = (Button) rootView.findViewById(R.id.multiplyButton);

        ArrayAdapter<CharSequence> adapter = ArrayAdapter.createFromResource(getContext(),
                R.array.numbers, android.R.layout.simple_spinner_item);
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        rowsSpinner.setAdapter(adapter);
        columnSpinner.setAdapter(adapter);
        spinners.addView(rowsSpinner);
        spinners.addView(columnSpinner);


        rowsSpinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> adapterView, View view, int i, long l) {
                int oldNumber = numberOfRows;
                numberOfRows = Integer.parseInt((String) adapterView.getItemAtPosition(i));
                if (numberOfRows > oldNumber) {
                    for (int index = oldNumber; index < numberOfRows; index++) {
                        LinearLayout newRow = new LinearLayout(getContext());
                        newRow.setOrientation(LinearLayout.HORIZONTAL);
                        /*
                        ViewGroup.LayoutParams params = newRow.getLayoutParams();
                        params.height = ViewGroup.LayoutParams.WRAP_CONTENT;
                        params.width = ViewGroup.LayoutParams.WRAP_CONTENT;
                        newRow.setLayoutParams(params);
                        */

                        for (int index2 = 0; index2 < numberOfColumns; index2++) {
                            EditText et = new EditText(getContext());
                            et.setHint("Number");
                            et.setTextColor(Color.BLACK);
                            et.setHintTextColor(Color.BLACK);
                            et.setInputType(InputType.TYPE_CLASS_NUMBER);
                            newRow.addView(et);
                        }
                        matrix.addView(newRow);
                    }
                } else {
                    for (int index = oldNumber - 1; index >= numberOfRows; index--) {
                        matrix.removeViewAt(index);
                    }
                }
            }

            @Override
            public void onNothingSelected(AdapterView<?> adapterView) {

            }
        });
        columnSpinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> adapterView, View view, int i, long l) {
                int oldNumber = numberOfColumns;
                numberOfColumns = Integer.parseInt((String) adapterView.getItemAtPosition(i));
                if (numberOfColumns > oldNumber) {
                    for (int index = 0; index < numberOfRows; index++) {
                        LinearLayout current = (LinearLayout) matrix.getChildAt(index);
                        for (int index2 = oldNumber; index2 < numberOfColumns; index2++) {
                            EditText et = new EditText(getContext());
                            et.setHint("Number");
                            et.setTextColor(Color.BLACK);
                            et.setHintTextColor(Color.BLACK);
                            et.setInputType(InputType.TYPE_CLASS_NUMBER);
                            current.addView(et);
                        }

                    }
                } else {
                    for (int index = 0; index < numberOfRows; index++) {
                        LinearLayout current = (LinearLayout) matrix.getChildAt(index);
                        for (int index2 = oldNumber - 1; index2 >= numberOfColumns; index2--) {
                            current.removeViewAt(index2);
                        }
                    }
                }
            }

            @Override
            public void onNothingSelected(AdapterView<?> adapterView) {

            }
        });
        transposeButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                int[][] matrixArray;
                try {
                    matrixArray = layoutToArray(matrix);
                } catch (NumberFormatException e) {
                    return;
                }

                int[][] transposeArray = transpose(matrixArray);
                arrayToLayout(result, transposeArray);
            }
        });

        return rootView;
    }

    public int[][] layoutToArray(LinearLayout ll) {
        LinearLayout current = (LinearLayout) ll.getChildAt(0);
        int[][] array = new int[ll.getChildCount()][current.getChildCount()];

        for (int i = 0; i < array.length; i++) {
            current = (LinearLayout) ll.getChildAt(i);
            for (int j = 0; j < array[i].length; j++) {
                array[i][j] = Integer.parseInt(((TextView) current.getChildAt(j)).getText().toString());
            }
        }
        return array;
    }

    public void arrayToLayout(LinearLayout ll, int[][] array) {
        ll.removeAllViews();
        for (int i = 0; i < array.length; i++) {
            LinearLayout newRow = new LinearLayout(getContext());
            newRow.setOrientation(LinearLayout.HORIZONTAL);
            for (int j = 0; j < array[i].length; j++) {
                EditText et = new EditText(getContext());
                et.setHint("Number");
                et.setTextColor(Color.BLACK);
                et.setHintTextColor(Color.BLACK);
                et.setInputType(InputType.TYPE_CLASS_NUMBER);
                et.setText(String.valueOf(array[i][j]));
                newRow.addView(et);
            }
            ll.addView(newRow);
        }
    }

    public int[][] transpose(int[][] matrix) {
        int[][] transpose = new int[matrix[0].length][matrix.length];
        for (int i = 0; i < transpose.length; i++) {
            for (int j = 0; j < transpose[i].length; j++) {
                transpose[i][j] = matrix[j][i];
            }
        }
        return transpose;

    }

    public TransposeMatrix() {}

}