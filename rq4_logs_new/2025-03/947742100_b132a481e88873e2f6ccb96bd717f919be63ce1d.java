package board;

import lombok.Getter;
import lombok.Setter;
import post.Post;

import java.util.ArrayList;
import java.util.List;

@Getter @Setter
public class Board {
    private long boardId;
    private String boardName;
    private List<Post> posts;

    public Board(String boardName) {
        this.boardName = boardName;
        posts = new ArrayList<>();
    }

    public Board(long boardId, String boardName) {
        this.boardId = boardId;
        this.boardName = boardName;
        posts = new ArrayList<>();
    }
}