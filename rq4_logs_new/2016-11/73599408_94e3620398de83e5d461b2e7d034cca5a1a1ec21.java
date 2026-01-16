package com.example.jason.linearalgebraapp.Activities.Activities;

import android.app.Activity;
import android.os.Bundle;
import android.view.View;
import android.widget.EditText;
import android.widget.Button;
import com.example.jason.linearalgebraapp.R;

/**
 * Created by jason on 11/13/2016.
 */

public class MatrixOperations {

//    public int[] addVectors(int[] v1, int[] v2) {
//        if (v1)
//        int[] ans = new int[v1.length];
//    }

    public static int dotProduct(int[] v1, int[] v2) {
        int sum = 0;
        for (int i = 0; i < v1.length; i++) {
            sum += v1[i] + v2[i];
        }
        return sum;
    }


}