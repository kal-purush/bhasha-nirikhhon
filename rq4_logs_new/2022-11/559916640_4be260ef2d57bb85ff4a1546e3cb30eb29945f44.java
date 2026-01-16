package com.project.board.vo;

public class RiderBoardVo {
    private int board_number;
    private String menu_id;
    private String title;
    private String writer;
    private String board_local;
    private String cont;
    private String indate;
    private String board_check;
    private String r_start;
    private String r_end;
    private String money;
    private String r_time;
    private String luggage;

    public RiderBoardVo() {
    }

    public RiderBoardVo(int board_number, String menu_id, String title, String writer, String board_local, String cont, String indate, String board_check, String r_start, String r_end, String money, String r_time, String luggage) {
        this.board_number = board_number;
        this.menu_id = menu_id;
        this.title = title;
        this.writer = writer;
        this.board_local = board_local;
        this.cont = cont;
        this.indate = indate;
        this.board_check = board_check;
        this.r_start = r_start;
        this.r_end = r_end;
        this.money = money;
        this.r_time = r_time;
        this.luggage = luggage;
    }

    @Override
    public String toString() {
        return "RiderBoardVo{" +
                "board_number=" + board_number +
                ", menu_id='" + menu_id + '\'' +
                ", title='" + title + '\'' +
                ", writer='" + writer + '\'' +
                ", board_local='" + board_local + '\'' +
                ", cont='" + cont + '\'' +
                ", indate='" + indate + '\'' +
                ", board_check='" + board_check + '\'' +
                ", r_start='" + r_start + '\'' +
                ", r_end='" + r_end + '\'' +
                ", money='" + money + '\'' +
                ", r_time='" + r_time + '\'' +
                ", luggage='" + luggage + '\'' +
                '}';
    }
}