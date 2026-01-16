package com.project.board.vo;

public class ReviewVo {

    private int board_number;
    private String menu_id;
    private String title;
    private String writer;
    private String board_local;
    private String cont;
    private String indate;

    public ReviewVo() {
    }

    public ReviewVo(int board_number, String menu_id, String title, String writer, String board_local, String cont, String indate) {
        this.board_number = board_number;
        this.menu_id = menu_id;
        this.title = title;
        this.writer = writer;
        this.board_local = board_local;
        this.cont = cont;
        this.indate = indate;
    }

    public int getBoard_number() {
        return board_number;
    }

    public void setBoard_number(int board_number) {
        this.board_number = board_number;
    }

    public String getMenu_id() {
        return menu_id;
    }

    public void setMenu_id(String menu_id) {
        this.menu_id = menu_id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getWriter() {
        return writer;
    }

    public void setWriter(String writer) {
        this.writer = writer;
    }

    public String getBoard_local() {
        return board_local;
    }

    public void setBoard_local(String board_local) {
        this.board_local = board_local;
    }

    public String getCont() {
        return cont;
    }

    public void setCont(String cont) {
        this.cont = cont;
    }

    public String getIndate() {
        return indate;
    }

    public void setIndate(String indate) {
        this.indate = indate;
    }

    @Override
    public String toString() {
        return "ReviewVo{" +
                "board_number=" + board_number +
                ", menu_id='" + menu_id + '\'' +
                ", title='" + title + '\'' +
                ", writer='" + writer + '\'' +
                ", board_local='" + board_local + '\'' +
                ", cont='" + cont + '\'' +
                ", indate='" + indate + '\'' +
                '}';
    }

}