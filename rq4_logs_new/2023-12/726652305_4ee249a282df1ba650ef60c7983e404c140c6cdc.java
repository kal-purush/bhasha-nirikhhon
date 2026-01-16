package ZZEstreams.test;

import ZZEstreams.dominio.LightNovel;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class StreamTest03 {
    private static List<LightNovel> lightNovels = new ArrayList<>(List.of(
            new LightNovel("Naruto Shippuden", 4.90),
            new LightNovel("Dragon Ball Super", 5.40),
            new LightNovel("One Piece", 3.00),
            new LightNovel("Bleach", 2.20),
            new LightNovel("Bleach", 2.20),
            new LightNovel("Fullmetal Alchimist", 6.00),
            new LightNovel("One Punch Man", 2.90)
    ));

    public static void main(String[] args) {
        Stream<LightNovel> stream = lightNovels.stream();
        lightNovels.forEach(System.out::println);

        long count = stream
                .distinct()
                .filter(ln -> ln.getPrice() <= 4)
                .count();

        System.out.println(count);
    }
}