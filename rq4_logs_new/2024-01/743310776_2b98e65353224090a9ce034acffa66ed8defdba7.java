package com.vmelnyk.geeks4geeks._20240116AllUniquePermutationsOfArray;

import com.vmelnyk.Util;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class BenchMark {
  @State(Scope.Benchmark)
  public static class ExecutionPlan {

    @Param({"5", "4", "3", "2"})
    public int iterations;

    public Solution solution;

    @Setup(Level.Invocation)
    public void setUp() {
      solution = new Solution();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(value = 1, warmups = 1)
  @Measurement(iterations = 2, time = 5)
  @Warmup(iterations = 1, time = 5)
  /*public void benchPermuteUniqueArrays(ExecutionPlan plan) {
    for (int i = plan.iterations; i > 0; i--) {
      plan.solution.permuteUniqueArrays(new ArrayList<>(Util.randomIntList(i)), i);
    }
  }*/
  public void benchPermuteUniqueArrays() {
    Solution solution = new Solution();
    solution.permuteUniqueArrays(new ArrayList<>(Util.randomIntList(10)), 10);
  }

  public static void main(String[] args) throws RunnerException {
    Options options =
        new OptionsBuilder()
            .include(BenchMark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();
    new Runner(options).run();
  }
}