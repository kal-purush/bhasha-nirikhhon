package com.rms.drifeserver.integrationTest;

import com.rms.drifeserver.domain.board.dao.BoardRepository;
import com.rms.drifeserver.domain.board.model.Board;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class SetupBoardIntegrationTest extends SetupUserIntegrationTest {

    @Autowired
    protected BoardRepository boardRepository;

    protected Long boardId;

    protected Board board;

    @BeforeEach
    void setupBoard() {

        String title = "test-title";
        String contents = "test-contents";
        List<String> imageUrls = new ArrayList<>(Arrays.asList("test1.png","test2.png"));

        board = Board.of(user, title, contents, imageUrls);

        boardRepository.save(board);
        boardId = board.getId();
    }
}