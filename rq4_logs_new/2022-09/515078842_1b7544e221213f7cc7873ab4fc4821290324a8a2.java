package io.metadevs.akrasilnikov.module_2_9_DoubleLinkBilateralList;

import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public class DoubleLinkBilateralListAsymptoticComplexityTest {
    @Param({"1001", "10001", "100001"})
    int value;

    DoubleLinkBilateralList doubleLinkBilateralList;

    @Setup(Level.Invocation)
    public void prepare() {
        doubleLinkBilateralList = new DoubleLinkBilateralList();
        for (int i = 0; i <= value - 1; i++) {
            doubleLinkBilateralList.insertLast(i);
        }
    }

    @Benchmark
    public void InsertFirst() {
        doubleLinkBilateralList.insertFirst(value);
    }

    @Benchmark
    public void InsertLast() {
        doubleLinkBilateralList.insertLast(value);
    }

    @Benchmark
    public void InsertAfter() {
        doubleLinkBilateralList.insertAfter(0, value);
    }

    @Benchmark
    public void DeleteFirst() {
        doubleLinkBilateralList.deleteFirst();
    }

    @Benchmark
    public void DeleteLast() {
        doubleLinkBilateralList.deleteLast();
    }

    @Benchmark
    public void DeleteKeyWith() {
        doubleLinkBilateralList.deleteKey(50);
    }
}