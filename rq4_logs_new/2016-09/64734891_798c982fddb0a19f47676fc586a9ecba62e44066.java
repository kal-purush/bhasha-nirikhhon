import com.marvin.component.kernel.Kernel;
import javax.swing.Action;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import marvin.demo.app.AppKernel;
import com.marvin.bundle.swing.ApplicationAction;
import com.marvin.bundle.swing.MarvinSwingApplication;

public class TestApplication extends MarvinSwingApplication {
    
    public static void main(String[] args) {
        Kernel kernel = new AppKernel(true);
        MarvinSwingApplication app = new TestApplication(kernel);
        app.show();
    }

    public TestApplication(Kernel kernel) {
        super(kernel);
    }

    @Override
    protected JMenuBar buildMenuBar() {
        JMenuBar menuBar = new JMenuBar();
        JMenu test = new JMenu(new ApplicationAction("test", getHandler()));
        JMenuItem item = new JMenuItem(new ApplicationAction("item", getHandler()));
        test.add(item);
        menuBar.add(test);
        return menuBar;
    }

    @Override
    protected void buildFrame(JFrame frame) {
    }
}