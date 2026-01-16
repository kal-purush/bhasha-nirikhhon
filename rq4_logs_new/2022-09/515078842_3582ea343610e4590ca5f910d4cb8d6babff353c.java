package io.metadevs.akrasilnikov.module_2_9_DoubleLinkBilateralList;

import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public class IteratorAsymptoticComplexityTest {
    @Param({"1001", "10001", "100001"})
    int value;

    DoubleLinkBilateralList doubleLinkBilateralList;
    ListIterator listIterator;

    @Setup(Level.Invocation)
    public void prepare() {
        doubleLinkBilateralList = new DoubleLinkBilateralList();
        listIterator = doubleLinkBilateralList.getIterator();
        for (int i = 0; i < value-1; i++) {
            listIterator.insertAfter(i);
        }
    }

    @Benchmark
    public void iteratorInsertAfter() {
        listIterator.insertAfter(20);
    }

    @Benchmark
    public void iteratorInsertBefore() {
        listIterator.insertBefore(20);
    }
}