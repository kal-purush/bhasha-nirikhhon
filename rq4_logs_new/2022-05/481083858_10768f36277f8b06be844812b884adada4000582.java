import beans.BoardDto;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;

public class Pagination {
    int currentpage = 1;
    int postsperpate = 10;
    int indexoflast = currentpage * postsperpate;
    int indexoffirst = indexoflast - postsperpate;

    public int getCurrentpage() {
        return currentpage;
    }

    public void setCurrentpage(int currentpage) {
        this.currentpage = currentpage;
    }

    public int getPostsperpate() {
        return postsperpate;
    }

    public void setPostsperpate(int postsperpate) {
        this.postsperpate = postsperpate;
    }

    public int getIndexoflast() {
        return indexoflast;
    }

    public void setIndexoflast(int indexoflast) {
        this.indexoflast = indexoflast;
    }

    public int getIndexoffirst() {
        return indexoffirst;
    }

    public void setIndexoffirst(int indexoffirst) {
        this.indexoffirst = indexoffirst;
    }

//    public ArrayList currentposts(ArrayList arr){
//
//    }
}