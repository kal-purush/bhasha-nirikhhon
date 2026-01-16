package io.metadevs.akrasilnikov.module_2_4_ArrayList;

import io.metadevs.akrasilnikov.OOP.*;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public class ArrayListAsymptoticComplexity {
    AbstractSpecialist bob = new Worker("Bob", 21);
    ArrayList<AbstractSpecialist> arrayListPersons;

    @Param({"1001", "10001", "100001"})
    public int value;

    @Benchmark
    public void add() {
       // System.out.println("DO Add" + value);
        arrayListPersons = new ArrayList<AbstractSpecialist>(value);
        for (int i = 0; i < value - 1; i++) {
            arrayListPersons.add(bob);
        }

    }

    @Benchmark
    public void addByIndex() {
       // System.out.println("DO AddIndex" + value);
        arrayListPersons = new ArrayList<AbstractSpecialist>(value);
        for (int i = 0; i < value - 1; i++) {
            arrayListPersons.add(value, bob);
        }
    }

    @Benchmark
    public void addAndRemove() {
        //System.out.println("DO AddAndRemove" + value);
        arrayListPersons = new ArrayList<AbstractSpecialist>(value);
        for (int i = 0; i < value - 1; i++) {
            arrayListPersons.add(bob);
        }
        for (int i = 0; i < value - 1; i++) {
            arrayListPersons.remove(0);
        }

    }
}