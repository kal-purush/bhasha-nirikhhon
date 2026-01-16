package crawling.exception;

import crawling.Main;

/**
 * Created by midori on 2016/05/08.
 */
public class FromBeginningException extends Exception{
    @Override
    public String getMessage() {
        return Main.ctrl + "-------- 初めからやり直します --------" + Main.ctrl + Main.ctrl;
    }
}