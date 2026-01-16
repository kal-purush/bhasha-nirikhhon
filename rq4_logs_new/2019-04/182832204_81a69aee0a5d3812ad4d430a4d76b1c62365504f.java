package C1140;

import static java.math.BigInteger.ZERO;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public class Songs {

    public static void main(String[] args) {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        Integer maxSong = Integer.valueOf(input.lines()
                                               .findFirst()
                                               .orElseThrow(IllegalArgumentException::new)
                                               .split(" ")[1]);
        List<Song> songs = input.lines()
                                .map(s -> s.split(" "))
                                .map(s -> new Song(BigInteger.valueOf(Integer.valueOf(s[0])),
                                                   BigInteger.valueOf(Integer.valueOf(s[1]))))

                                .collect(toList());
        System.out.println(compute(songs, maxSong));
    }

    public static BigInteger compute(List<Song> songs, Integer maxSong) {
        Comparator<Song> comparing = comparing(s -> s.time);
        List<Song> songByTime = new ArrayList<>(songs);
        songByTime.sort(comparing.reversed());
        List<IndexedSong> indexedSongsByTime = IntStream.range(0, songs.size())
                                             .mapToObj(i -> new IndexedSong(songByTime.get(i).time, songByTime.get(i).beauty, i))
                                             .collect(toList());
        List<IndexedSong> songByBeauty = new ArrayList<>(indexedSongsByTime);
        songByBeauty.sort(comparing(s -> s.beauty));

        BigInteger sumtime = ZERO;
        int timeIndex = -1;
        for (int i = 0; i < maxSong - 1; i++) {
            sumtime = sumtime.add(indexedSongsByTime.get(i).time);
            timeIndex = i;
        }

        BigInteger bestSum = ZERO;
        for (IndexedSong song : songByBeauty) {
            song.removed = true;
            if (song.index <= timeIndex) {
                sumtime = sumtime.subtract(song.time);
                while (timeIndex < songs.size() - 1) {
                    timeIndex++;
                    if (indexedSongsByTime.get(timeIndex).removed) {
                        continue;
                    }
                    if (indexedSongsByTime.get(timeIndex).beauty.compareTo(song.beauty) >= 0) {
                        sumtime = sumtime.add(indexedSongsByTime.get(timeIndex).time);
                        break;
                    }
                }
            }
            BigInteger score = song.time.add(sumtime).multiply(song.beauty);
            if (score.compareTo(bestSum) > 0) {
                bestSum = score;
            }
        }
        return bestSum;
    }

    static class Song {
        public final BigInteger time;
        public final BigInteger beauty;

        public static Song song(Integer time, Integer beauty) {
            return new Song(BigInteger.valueOf(time), BigInteger.valueOf(beauty));
        }

        Song(BigInteger time, BigInteger beauty) {
            this.time = time;
            this.beauty = beauty;
        }
    }

    static class IndexedSong {
        public final BigInteger time;
        public final BigInteger beauty;
        public final int index;
        private boolean removed = false;

        public static IndexedSong song(Integer time, Integer beauty, int index) {
            return new IndexedSong(BigInteger.valueOf(time), BigInteger.valueOf(beauty), index);
        }

        IndexedSong(BigInteger time, BigInteger beauty, int index) {
            this.time = time;
            this.beauty = beauty;
            this.index = index;
        }
    }
}