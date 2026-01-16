package com.example.a327finalprojectcalorietracker;

import androidx.lifecycle.ViewModel;

public class sharedView extends ViewModel {
    private float budgetCalVal;

    public float getBudgetCalVal() {
        return budgetCalVal;
    }

    public void setBudgetCalVal(float budgetCalVal) {
        this.budgetCalVal = budgetCalVal;
    }
}