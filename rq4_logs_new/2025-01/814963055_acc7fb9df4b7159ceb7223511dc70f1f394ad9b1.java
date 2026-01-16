package com.example.multithreadingProject.WeightMachineAdapter;

import com.example.multithreadingProject.WeightMachineAdapter.adapter.WeightMachineAdapterImpl;
import com.example.multithreadingProject.WeightMachineAdapter.weightMachine.InfantWeightMachine;

public class WeightMachineAdapterApplication {
    public static void main(String[] args) {
        InfantWeightMachine infantWeightMachine = new InfantWeightMachine();
        WeightMachineAdapterImpl weightMachineAdapter = new WeightMachineAdapterImpl(infantWeightMachine);
        System.out.println(weightMachineAdapter.getWeightInKg());
    }
}