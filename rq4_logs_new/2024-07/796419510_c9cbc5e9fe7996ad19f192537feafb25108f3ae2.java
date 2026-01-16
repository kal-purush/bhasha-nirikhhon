package benchmark.jal.collections.queue;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.jal.collections.queue.HeapPriorityQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class HeapPriorityQueueBenchmark {
  HeapPriorityQueue<Integer> queue;

  @Param({"128", "256", "512", "1024", "2048", "4096", "8192", "16384", "32768", "65536", "131072"})
  int size;

  @Benchmark
  public HeapPriorityQueue<Integer> measureAdd() {
    this.queue = new HeapPriorityQueue<>(Comparator.comparing(v -> v));
    for (int i = 0; i < this.size; ++i) {
      this.queue.add(i);
    }

    return this.queue;
  }

  @Benchmark
  public HeapPriorityQueue<Integer> measureAddAndPoll() {
    this.queue = new HeapPriorityQueue<>(Comparator.comparing(v -> v));
    for (int i = 0; i < this.size; ++i) {
      this.queue.add(i);
    }
    for (int i = 0; i < this.size; ++i) {
      this.queue.poll();
    }

    return this.queue;
  }
}