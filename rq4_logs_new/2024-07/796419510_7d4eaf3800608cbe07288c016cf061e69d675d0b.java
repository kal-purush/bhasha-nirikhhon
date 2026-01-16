package org.jal.apps;

import java.util.ArrayList;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

record Dataset(String label, ArrayList<Integer> data) {};
record Func(String name, IntUnaryOperator f) {};
record HashesSet(String funcName, ArrayList<Dataset> datasets) {};

public class HashDiff {
  private static double GOLDEN = 1.618033988749;
  private static int WORD_GOLDEN = 1327217885;
  private static int SIZE = 32;
  private static int SIZE_POWER = 5;
  private static Collector<Integer, ?, ArrayList<Integer>> ARR_COL = Collectors.toCollection(ArrayList::new);
  private static int FORMAT_ROW = 8;

  public static void main() {
    var datasets = generateDatasets();
    var funcs = generateHashFunctions();

    var hashesSets = getHashesSets(datasets, funcs);
    printHashes(hashesSets);
  }

  private static ArrayList<Dataset> generateDatasets() {
    ArrayList<Dataset> datasets = new ArrayList<>();
    datasets.add(new Dataset(
      "first half", Stream.iterate(0, i->i+1).limit(SIZE/2).collect(ARR_COL)
    ));
    datasets.add(new Dataset(
      "multiplies of 2", Stream.iterate(0, i->i+2).limit(SIZE*4).collect(ARR_COL)
    ));
    datasets.add(new Dataset(
      "multiplies of 4", Stream.iterate(0, i->i+4).limit(SIZE*4).collect(ARR_COL)
    ));
    datasets.add(new Dataset(
      "multiplies of size", Stream.iterate(0, i->i+SIZE).limit(SIZE*4).collect(ARR_COL)
    ));
    datasets.add(new Dataset(
      "multiplies of golden ratio", Stream.iterate(0, i->i+1).map(i -> (int)(i*GOLDEN)).limit(SIZE*4).collect(ARR_COL)
    ));

    return datasets;
  }

  private static ArrayList<Func> generateHashFunctions() {
    ArrayList<Func> hashFuncs = new ArrayList<>();
    hashFuncs.add(new Func(
      "modular hashing", n -> n % SIZE
    ));
    hashFuncs.add(new Func(
      "fibonacci hashing (multiplication)", n -> (int)(SIZE*((n/GOLDEN)%1))
    ));
    hashFuncs.add(new Func(
      "fibonacci hashing (shift)", n -> (int)((n*WORD_GOLDEN)&0x7FFFFFFF) >> (31-SIZE_POWER)
    ));

    return hashFuncs;
  }

  private static ArrayList<HashesSet> getHashesSets(ArrayList<Dataset> datasets, ArrayList<Func> hashFuncs) {
    ArrayList<HashesSet> sets = new ArrayList<>();

    for (var f : hashFuncs) {
      sets.add(new HashesSet(f.name(), getHashesForFunc(datasets, f.f())));
    }

    return sets;
  }

  private static ArrayList<Dataset> getHashesForFunc(ArrayList<Dataset> datasets, IntUnaryOperator hashFunc) {
    ArrayList<Dataset> hashes = new ArrayList<>();
    for (var dataset : datasets) {
      hashes.add(getHashes(dataset, hashFunc));
    }

    return hashes;
  }

  private static Dataset getHashes(Dataset dataset, IntUnaryOperator hashFunc) {
    String label = dataset.label();
    ArrayList<Integer> nums = dataset.data();

    return new Dataset(label, StreamSupport.stream(nums.spliterator(), false).map(hashFunc::applyAsInt).collect(ARR_COL));
  }

  private static void printDatasets(ArrayList<Dataset> datasets) {
    for (Dataset d : datasets) {
      System.out.println("  " + "Dataset: " + d.label());

      printDataset(d.data());
    }
  }

  private static void printDataset(ArrayList<Integer> dataset) {
    int[] counts = new int[SIZE];

    for (int n : dataset) {
      counts[n]++;
    }

    for (int i = 0; i < SIZE; ++i) {
      if (i > 0 && (i % FORMAT_ROW) == 0) { // break line
        System.out.println();
      }
      if ((i % FORMAT_ROW) == 0) { // indent
        System.out.print("  ");
      }

      int c = counts[i];
      System.out.print(String.format("%3s", c) + " ");
    }
    System.out.println();
  }

  private static void printHashes(ArrayList<HashesSet> hashesSets) {
    for (HashesSet s : hashesSets) {
      System.out.println("Hash function: " + s.funcName());

      printDatasets(s.datasets());
    }
  }
}