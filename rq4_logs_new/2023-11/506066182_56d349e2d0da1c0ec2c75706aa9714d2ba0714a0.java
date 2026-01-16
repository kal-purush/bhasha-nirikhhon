import java.util.List;

public class VowelDictionaryLoops {
    public enum Index {
        First(0),
        Second(1),
        Third(2),
        Fourth(3),
        Fifth(4);

        public final int value;

        Index(int value) {
            this.value = value;
        }
    }

    public int solution(String word) {
        List<Character> vowels = List.of('A', 'E', 'I', 'O', 'U');
        int order = 0;

        StringBuilder stringBuilder = new StringBuilder();

        for (Character first : vowels) {
            order += 1;
            if (createdWordEquals(stringBuilder, first, Index.First, word)) {
                return order;
            }

            for (Character second : vowels) {
                order += 1;
                if (createdWordEquals(stringBuilder, second, Index.Second, word)) {
                    return order;
                }

                for (Character third : vowels) {
                    order += 1;
                    if (createdWordEquals(stringBuilder, third, Index.Third, word)) {
                        return order;
                    }

                    for (Character fourth : vowels) {
                        order += 1;
                        if (createdWordEquals(stringBuilder, fourth, Index.Fourth, word)) {
                            return order;
                        }

                        for (Character fifth : vowels) {
                            order += 1;
                            if (createdWordEquals(stringBuilder, fifth, Index.Fifth, word)) {
                                return order;
                            }
                        }

                        stringBuilder.deleteCharAt(Index.Fourth.value);
                    }

                    stringBuilder.deleteCharAt(Index.Third.value);
                }

                stringBuilder.deleteCharAt(Index.Second.value);
            }

            stringBuilder.deleteCharAt(Index.First.value);
        }

        return order;
    }

    private boolean createdWordEquals(StringBuilder stringBuilder,
                                      Character character,
                                      Index index,
                                      String word) {
        if (character != 'A') {
            stringBuilder.deleteCharAt(index.value);
        }

        String other = stringBuilder
            .append(character)
            .toString();

        return other.equals(word);
    }
}