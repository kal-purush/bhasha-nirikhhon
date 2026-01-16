package org.jal.sorting.strategy.arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Comparator;

import org.jal.sorting.ArraySorter;
import org.junit.jupiter.api.Test;

public class InsertionStrategyTestExample {
  @Test
  public void testSort() {
    Integer[] toSort = { 20, 30, 10, 40 };
    Integer[] expected = { 10, 20, 30, 40 };
    ArraySortStrategy<Integer> strat = new InsertionStrategy<>();

    ArraySorter.sortArray(toSort, strat);

    assertArrayEquals(expected, toSort);
  }

  @Test
  public void testRem8DistinctSort() {
    Integer[] toSort = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    Integer[] expected = { 0, 8, 1, 9, 2, 10, 3, 4, 5, 6, 7 };
    ArraySortStrategy<Integer> strat = new InsertionStrategy<>();

    ArraySorter.sortArray(toSort, strat, Comparator.comparing(v -> v % 8));

    assertArrayEquals(expected, toSort);
  }

  @Test
  public void testRem8DuplicateSort() {
    Integer[] toSort = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    Integer[] expected = { 0, 8, 1, 9, 2, 10, 3, 4, 5, 6, 7 };
    ArraySortStrategy<Integer> strat = new InsertionStrategy<>();

    ArraySorter.sortArray(toSort, strat,
      Comparator.comparing(v -> v % 8));

    assertArrayEquals(expected, toSort);
  }

  class AnimalRecord {
    public String species;
    public String name;

    public AnimalRecord(String species, String name) {
      this.species = species;
      this.name = name;
    }
  }
    @Test
  public void testAnimalRecordSort() {
    final AnimalRecord[] recs = {
      new AnimalRecord("Dog", "3"),
      new AnimalRecord("Dog", "2"),
      new AnimalRecord("Dog", "1"),
      new AnimalRecord("Cat", "A"),
      new AnimalRecord("Cat", "B"),
      new AnimalRecord("Cat", "C"),
    };

    AnimalRecord[] toSort = { recs[0], recs[5], recs[3], recs[1], recs[4], recs[2] };
    AnimalRecord[] expected = recs;

    ArraySortStrategy<AnimalRecord> strat = new InsertionStrategy<>();

    ArraySorter.sortArray(toSort, strat, (rec1, rec2) -> {
      int speciesOrder1 = rec1.species.equals("Dog") ? 0 : 1;
      int speciesOrder2 = rec2.species.equals("Dog") ? 0 : 1;

      if (speciesOrder1 == 0 || speciesOrder2 == 0) {
        return speciesOrder1 - speciesOrder2; // follow species ordering
      }

      return rec1.name.compareTo(rec2.name);
    });

    assertArrayEquals(expected, toSort);
  }
}