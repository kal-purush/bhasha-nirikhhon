package benchmark.jal.collections.dictionary;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.jal.collections.dictionary.DoubleHashingTable;
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
public class DoubleHashingTableGetBenchmark {
  private DoubleHashingTable<Integer, Integer> table;
  private Random rand = new Random();
  private Integer[] nums;

  // load factor 0.375
  @Param({"48", "96", "192", "384", "768", "1536", "3072", "6144", "12288", "24576", "49152", "98304"})
  int size;

  @Setup(Level.Iteration)
  public void setup() {
    // initialize shuffled numbers
    Set<Integer> nums = new HashSet<>();
    while (nums.size() < this.size) {
      nums.add(this.rand.nextInt(Integer.MAX_VALUE));
    }
    this.nums = StreamSupport.stream(nums.spliterator(), false).toArray(Integer[]::new);

    // initialize a table
    this.table = new DoubleHashingTable<>(v -> v);
    for (int num : nums) {
      this.table.set(num);
    }
  }

  @Benchmark
  public int measureGet() {
    int key = this.nums[this.rand.nextInt(this.size)];
    int value = this.table.get(key);
    return value;
  }
}