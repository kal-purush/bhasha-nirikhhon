package com.project.board.vo;

import org.springframework.stereotype.Controller;

import java.util.Timer;

public class BoardPager {

    private int totalCount;   // 게시글 총 개수
    private int pageNum;      // 현재 페이지 번호
    private int contentNum;   // 한 페이지에 보여줄 게시글 개수
    private int startPage=1;  // 현재 페이지 블록 시작 번호
    private int endPage=5;    // 현재 페이지 블록 종료 번호
    private boolean prev;     // 이전 페이지로 가는 화살표
    private boolean next;     // 다음 페이지로 가는 화살표
    private int currentBlock; // 다음 페이지로 가는 화살표
    private double lastBlock;    // 다음 페이지로 가는 화살표

    public void prevNext(int pageNum){ // 이전, 다음 페이지 블록

        if(calcPage(totalCount)<6){
            setPrev(false);
            setNext(false);
        }else if (pageNum>0 && pageNum<=5){
            setPrev(false);
            setNext(true);
        }else if (lastBlock == currentBlock){
            setPrev(true);
            setNext(false);
        }else {
            setPrev(true);
            setNext(true);
        }
    }
    public int calcPage(int totalCount){ // 전체페이지 개수를 계산하는 함수
        //    12.5    =     125    /     10
        //   13페이지
        int totalPage = totalCount / 10;

        return totalPage;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getContentNum() {
        return contentNum;
    }

    public void setContentNum(int contentNum) {
        this.contentNum = contentNum;
    }

    public int getStartPage() {
        return startPage;
    }

    public void setStartPage(int startPage) {
        this.startPage = (currentBlock * 5) - 4;
    }

    public int getEndPage() {
        return endPage;
    }

    public void setEndPage() { // 마지막 페이지 블록 구하는 곳
        if (lastBlock == currentBlock){
            endPage = totalCount / 10;
            if (0< (totalCount % 10)){
                endPage++;
            }
        }else {
            endPage = getStartPage() + 4;
        }
    }
    public boolean isPrev() {
        return prev;
    }

    public void setPrev(boolean prev) {
        this.prev = prev;
    }

    public boolean isNext() {
        return next;
    }

    public void setNext(boolean next) {
        this.next = next;
    }

    public int getCurrentBlock() {
        return currentBlock;
    }

    public void setCurrentBlock(int pageNum) {
        // 현제페이지 블록 구하는 곳, 페이지 번호를 통해 구한다
        // 페이지 번호 / 페이지 그룹안의 페이지 개수
        this.currentBlock = pageNum/5;
        if(pageNum%5 > 0) {
            this.currentBlock++;
        }
    }

    public double getLastBlock() {
        return lastBlock;
    }

    public void setLastBlock() {
        lastBlock = totalCount / 50;
        if (totalCount % (5 * contentNum) > 0){
            lastBlock++;
        }
    }

    @Override
    public String toString() {
        return "BoardPager{" +
                "totalCount=" + totalCount +
                ", pageNum=" + pageNum +
                ", contentNum=" + contentNum +
                ", startPage=" + startPage +
                ", endPage=" + endPage +
                ", prev=" + prev +
                ", next=" + next +
                ", currentBlock=" + currentBlock +
                ", lastBlock=" + lastBlock +
                '}';
    }
}