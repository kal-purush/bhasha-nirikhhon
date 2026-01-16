package java8.ch02.ex12;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class CountWord {

	public static void main(String[] args) {
		Stream<String> words = Stream.of("a", "ab", "abc", "bca");
		AtomicInteger[] shortWords = counter(words);
		System.out.println(Arrays.toString(shortWords));
	}

	public static AtomicInteger[] counter(Stream<String> words) {
		AtomicInteger[] shortWords = new AtomicInteger[12];
		for (int i = 0; i < 12; i++) {
			shortWords[i] = new AtomicInteger();
		}
		words.parallel().forEach(s -> {
			if (s.length() < 12)
				shortWords[s.length()].incrementAndGet();
		});
		return shortWords;
	}

}