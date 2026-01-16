import javax.swing.*;

/**
 * StuMisMain  class
 *
 * @author Ethantse
 * @date 2018/12/28
 */

public class StuMisMain {
    public static void main(String[] args) {

        String lookAndFeel = UIManager.getSystemLookAndFeelClassName();
        try {
            UIManager.setLookAndFeel(lookAndFeel);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (UnsupportedLookAndFeelException e) {
            e.printStackTrace();
        }
        new StuMisMainFunction();
    }
}