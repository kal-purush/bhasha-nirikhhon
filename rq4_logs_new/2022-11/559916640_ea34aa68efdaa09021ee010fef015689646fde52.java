package com.project.board.vo;

public class BoardPagingVo {

    private int idx;
    private String title;
    private String cont;
    private String regdate;
    private int readcount;
    private String writer;

    private int filescount;

    private int bnum;
    private int lvl;
    private int step;
    private int nref;
    private int delnum;

    public int getFilescount() {
        return filescount;
    }
    public void setFilescount(int filescount) {
        this.filescount = filescount;
    }
    private String menu_id;
    private String menu_name;
    private String menu_seq;

    private String file_num;
    private String filename;
    private String fileext;
    private String sfilename;

    //paging 처리를 위해 추가
    private int nowpage; //현재페이지
    private int prevnowpage; //이전 10개를 클릭시 적용되는 nowpage
    private int nextnowpage; //다음 10개를 클릭시 적용되는  nowpage

    private int totalcount; //전체 ROW 수
    private int totalpagecount; //화면에 보여줄 페이지 수
    private int pagestartnum; //화면에 보여줄 페이지 시작 숫자
    private int pageendnum; //화면에 보여줄 페이지 마지막 숫자
    private int pagegrpnum; //페이지 그룹 숫자
    private int pagecount; //한페이지에 보여줄 ROW 숫자

    private boolean isshowpageprev;
    private boolean isshowpagenext;

    // Getter /setter
    public int getIdx() {
        return idx;
    }
    public void setIdx(int idx) {
        this.idx = idx;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public String getCont() {
        return cont;
    }
    public void setCont(String cont) {
        this.cont = cont;
    }
    public String getRegdate() {
        return regdate;
    }
    public void setRegdate(String regdate) {
        this.regdate = regdate;
    }
    public String getWriter() {
        return writer;
    }
    public void setWriter(String writer) {
        this.writer = writer;
    }
    public int getReadcount() {
        return readcount;
    }
    public void setReadcount(int readcount) {
        this.readcount = readcount;
    }
    public int getBnum() {
        return bnum;
    }
    public void setBnum(int bnum) {
        this.bnum = bnum;
    }
    public int getLvl() {
        return lvl;
    }
    public void setLvl(int lvl) {
        this.lvl = lvl;
    }
    public int getStep() {
        return step;
    }
    public void setStep(int step) {
        this.step = step;
    }
    public int getNref() {
        return nref;
    }
    public void setNref(int nref) {
        this.nref = nref;
    }
    public int getDelnum() {
        return delnum;
    }
    public void setDelnum(int delnum) {
        this.delnum = delnum;
    }
    public String getMenu_id() {
        return menu_id;
    }
    public void setMenu_id(String menu_id) {
        this.menu_id = menu_id;
    }
    public String getMenu_name() {
        return menu_name;
    }
    public void setMenu_name(String menu_name) {
        this.menu_name = menu_name;
    }
    public String getMenu_seq() {
        return menu_seq;
    }
    public void setMenu_seq(String menu_seq) {
        this.menu_seq = menu_seq;
    }
    public String getFile_num() {
        return file_num;
    }
    public void setFile_num(String file_num) {
        this.file_num = file_num;
    }
    public String getFilename() {
        return filename;
    }
    public void setFilename(String filename) {
        this.filename = filename;
    }
    public String getFileext() {
        return fileext;
    }
    public void setFileext(String fileext) {
        this.fileext = fileext;
    }
    public String getSfilename() {
        return sfilename;
    }
    public void setSfilename(String sfilename) {
        this.sfilename = sfilename;
    }


    public int getNowpage() {
        return nowpage;
    }
    public void setNowpage(int nowpage) {
        this.nowpage = nowpage;
    }
    public int getTotalcount() {
        return totalcount;
    }
    public void setTotalcount(int totalcount) {
        this.totalcount = totalcount;
    }

    public int getTotalpagecount() {
        return totalpagecount;
    }
    public void setTotalpagecount(int totalpagecount) {
        this.totalpagecount = totalpagecount;
    }
    public int getPagestartnum() {
        return pagestartnum;
    }
    public void setPagestartnum(int pagestartnum) {
        this.pagestartnum = pagestartnum;
    }
    public int getPageendnum() {
        return pageendnum;
    }
    public void setPageendnum(int pageendnum) {
        this.pageendnum = pageendnum;
    }
    public boolean isIsshowpageprev() {
        return isshowpageprev;
    }
    public void setIsshowpageprev(boolean isshowpageprev) {
        this.isshowpageprev = isshowpageprev;
    }
    public boolean isIsshowpagenext() {
        return isshowpagenext;
    }
    public void setIsshowpagenext(boolean isshowpagenext) {
        this.isshowpagenext = isshowpagenext;
    }

    public int getPagegrpnum() {
        return pagegrpnum;
    }
    public void setPagegrpnum(int pagegrpnum) {
        this.pagegrpnum = pagegrpnum;
    }

    public int getPagecount() {
        return pagecount;
    }
    public void setPagecount(int pagecount) {
        this.pagecount = pagecount;
    }

    public int getPrevnowpage() {
        return prevnowpage;
    }
    public void setPrevnowpage(int prenowpage) {
        this.prevnowpage = prenowpage;
    }
    public int getNextnowpage() {
        return nextnowpage;
    }
    public void setNextnowpage(int nextnowpage) {
        this.nextnowpage = nextnowpage;
    }
}