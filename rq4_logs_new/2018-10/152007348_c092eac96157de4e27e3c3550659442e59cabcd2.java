package my.example.miniboard.vo;

import java.util.Date;

public class Board {
    private int boardId;
    private String boardTitle;
    private String boardContent;
    private String boardAuthor;
    private String boardUid;    // 게시글 작성자 id
    private String boardCid;    // 챔피언 번호
    private Date registerDate;
    private Date updateDate;

    public int getBoardId() {
        return boardId;
    }

    public void setBoardId(int boardId) {
        this.boardId = boardId;
    }

    public String getBoardTitle() {
        return boardTitle;
    }

    public void setBoardTitle(String boardTitle) {
        this.boardTitle = boardTitle;
    }

    public String getBoardContent() {
        return boardContent;
    }

    public void setBoardContent(String boardContent) {
        this.boardContent = boardContent;
    }

    public String getBoardAuthor() {
        return boardAuthor;
    }

    public void setBoardAuthor(String boardAuthor) {
        this.boardAuthor = boardAuthor;
    }

    public String getBoardUid() {
        return boardUid;
    }

    public void setBoardUid(String boardUid) {
        this.boardUid = boardUid;
    }

    public String getBoardCid() {
        return boardCid;
    }

    public void setBoardCid(String boardCid) {
        this.boardCid = boardCid;
    }

    public Date getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(Date registerDate) {
        this.registerDate = registerDate;
    }

    public Date getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(Date updateDate) {
        this.updateDate = updateDate;
    }
}