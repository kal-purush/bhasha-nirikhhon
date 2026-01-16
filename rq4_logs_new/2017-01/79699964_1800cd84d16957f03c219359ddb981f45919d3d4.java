package handlingHomePageNavigation;

import java.awt.Cursor;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.sql.SQLException;

import basePage.ConfirmRunTestSuite;
import basePage.HomePage;

public class ConfirmRunTestSuiteSelected implements ActionListener{

	@Override
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
		ConfirmRunTestSuite.ConfirmRunTestSuitePointer.confirmRunTestSuiteFrame.setVisible(false);
		try {
			//ConfirmRunTestSuite.ConfirmRunTestSuitePointer.yesButton.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			HomePage.homePagePointer.runTestSuite();
			//ConfirmRunTestSuite.ConfirmRunTestSuitePointer.yesButton.setCursor(Cursor.getDefaultCursor());
		} catch (ClassNotFoundException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}