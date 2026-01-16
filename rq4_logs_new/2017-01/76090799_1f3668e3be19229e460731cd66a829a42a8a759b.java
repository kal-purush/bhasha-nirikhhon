package com.xxx.gogo;

public class BusEvent {

    public static class TabSwitcher{
        public static final int TAB_PROVIDER = 0;
        public static final int TAB_FAVO = 1;
        public static final int TAB_SHOPCART = 2;
        public static final int TAB_ME = 3;

        public int mPos;

        public TabSwitcher(int pos){
            mPos = pos;
        }
    }
}