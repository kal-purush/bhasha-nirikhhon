package com.dersommer.samples.java8;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.dersommer.samples.java8.dto.Person;
import com.dersommer.samples.java8.dto.PersonFactory;

public class StreamFilterTest {

    @Test
    public void test001_IntroduceStream() {
        List<Person> people = PersonFactory.generateList(10);

        Assert.assertNotNull(people);

        // Collections implement stream method. Returns a sequential Stream with this collection as its source.
        Stream<Person> stream = people.stream();

        Assert.assertEquals(10, stream.count());

        IntStream.range(0, 80)
            .forEach(i -> System.out.print("-"));

        // Print the contents, using a method reference
        people.forEach(System.out::println);

    }

    @Test
    public void test002_IntroduceFilter() {
        List<Person> people = PersonFactory.generateList(10);

        Assert.assertNotNull(people);

        // Collections implement stream method. Returns a sequential Stream with this collection as its source.
        Stream<Person> stream = people.stream();

        // Old school count. If we were searching for building a
        // filtered list in this method, we would need a new list instance
        int count = 0;
        for (Person p : people) {
            if (p.getRank() > 50)
                count++;
        }

        // New
        List<Person> filtered = stream.filter(p -> p.getRank() > 50)
            .collect(Collectors.toList());

        Assert.assertEquals(count, filtered.size());

        IntStream.range(0, 80)
            .forEach(i -> System.out.print("-"));
        System.out.println("");
        System.out.println(String.format("Filtered = %d items", filtered.size()));

        // Print the contents, using a method reference
        filtered.forEach(System.out::println);
    }

    @Test
    public void test003_FilterRemoveDuplicatesWithDistinct() {
        List<Person> people = PersonFactory.generateList(10);

        Assert.assertNotNull(people);

        // First, generate another list, using streams, of course
        // flatMap is a mapping function that may return one or more results, in a list.
        // After that, the results can be reduced or collected
        List<Person> duplicated = people.stream()
            .flatMap(p -> {
                ArrayList<Person> list = new ArrayList<>();
                list.add(p);
                list.add(p);

                return list.stream();
            })
            .collect(Collectors.toList());

        Assert.assertEquals(people.size() * 2, duplicated.size());

        // Order both lists, and apply a common filter, distinct()
        final List<Person> peopleCopy = people.stream()
            .distinct()
            .sorted((p1, p2) -> p1.getName()
                .compareTo(p2.getName()))
            .collect(Collectors.toList());

        final List<Person> filtered = duplicated.stream()
            .distinct()
            .sorted((p1, p2) -> p1.getName()
                .compareTo(p2.getName()))
            .collect(Collectors.toList());

        Assert.assertEquals(peopleCopy.size(), filtered.size());

        // Variables must be final or immutable to be captured inside the stream
        // some operations that run in parallel can't change the value of
        // variables from the external scope inside the stream
        // That is why we need "peopleCopy"
        IntStream.range(0, peopleCopy.size())
            .forEach(i -> {
                Assert.assertTrue(peopleCopy.get(i)
                    .equals(filtered.get(i)));
            });

        IntStream.range(0, 80)
            .forEach(i -> System.out.print("-"));
        System.out.println("");
        System.out.println(String.format("Filtered = %d items", filtered.size()));

        // Print the contents, using a method reference
        filtered.forEach(System.out::println);

    }

}