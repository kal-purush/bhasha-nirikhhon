package com.kuldeep.algorithims;

import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BoggleBoardTest {


    @Test
    public void shouldFindOutOfGivenWordsInBoggleBoard() {
        char[][] chars = {
                {'t', 'h', 'i', 's', 'i', 's', 'a'},
                {'s', 'i', 'm', 'p', 'l', 'e', 'x'},
                {'b', 'x', 'x', 'x', 'x', 'e', 'b'},
                {'x', 'o', 'g', 'g', 'l', 'x', 'o'},
                {'x', 'x', 'x', 'D', 'T', 'r', 'a'},
                {'R', 'E', 'P', 'E', 'A', 'd', 'x'},
                {'x', 'x', 'x', 'x', 'x', 'x', 'x'},
                {'N', 'O', 'T', 'R', 'E', '-', 'P'},
                {'x', 'x', 'D', 'E', 'T', 'A', 'E'}
        };

        BoggleBoard board = BoggleBoard.of(chars);

        List<String> found =
                board.findWords(List.of("this", "is", "not", "a", "simple", "boggle", "board", "test", "REPEATED", "NOTRE-PEATED"));

        assertThat(found)
                .isNotNull().isNotEmpty().hasSize(7)
                .containsExactlyInAnyOrder("this", "is", "a", "simple", "boggle", "board", "NOTRE-PEATED");
    }

}