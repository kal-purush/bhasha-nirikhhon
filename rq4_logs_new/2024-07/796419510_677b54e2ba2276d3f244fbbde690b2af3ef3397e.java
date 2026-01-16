package benchmark.jal.collections.dictionary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.jal.collections.dictionary.ChainingTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class ChainingTableGetBenchmark {
  private ChainingTable<Integer, Integer> table;
  private Random rand = new Random();

  @Param({"128", "256", "512", "1024", "2048", "4096", "8192", "16384", "32768", "65536", "131072"})
  int size;

  @Setup(Level.Iteration)
  public void setup() {
    // initialize shuffled numbers
    ArrayList<Integer> nums = new ArrayList<>(Stream.iterate(0, i->i+1).limit(this.size).toList());
    Collections.shuffle(nums, this.rand);

    // initialize a table
    this.table = new ChainingTable<>(v -> v);
    for (int num : nums) {
      this.table.set(num);
    }
  }

  @Benchmark
  public int measureGet() {
    int value = this.table.get(this.rand.nextInt(this.size));
    return value;
  }
}