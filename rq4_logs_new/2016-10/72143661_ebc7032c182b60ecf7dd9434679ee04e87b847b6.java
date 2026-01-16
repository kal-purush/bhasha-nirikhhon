
import javax.swing.JOptionPane;

public class Months {
public enum AccountType {January, Febuary, March, April, May, June, July, August, September, October, November, December}
public static void main(String[] args) {

	AccountType[] choices = { AccountType.January, AccountType.Febuary, AccountType.March, AccountType.April, AccountType.May, AccountType.June, AccountType.July, AccountType.August, AccountType.September, AccountType.October, AccountType.November, AccountType.December };
	AccountType select = (AccountType) JOptionPane.showInputDialog(null,"Select a month.", "List of months", JOptionPane.INFORMATION_MESSAGE, null, choices, choices[3]);

	while (select!=null) {
		switch (select) {
		case January:
			JOptionPane.showMessageDialog(null, "Do you want to build a snow man?");
			break;
		case Febuary:
			JOptionPane.showMessageDialog(null, "Do you want to build a snow man?");
			break;
		case March:
			JOptionPane.showMessageDialog(null, "Happy Spring days.");
			break;
		case April:
			JOptionPane.showMessageDialog(null, "Happy Spring days.");
			break;
		case May:
			JOptionPane.showMessageDialog(null, "Happy Spring days.");
			break;
		case June:
			JOptionPane.showMessageDialog(null, "It�s a summer time!");
			break;
		case July:
			JOptionPane.showMessageDialog(null, "It�s a summer time!");
			break;
		case August:
			JOptionPane.showMessageDialog(null, "It�s a summer time!");
			break;
		case September:
			JOptionPane.showMessageDialog(null, "Welcome to the foliage season.");
			break;
		case October:
			JOptionPane.showMessageDialog(null, "Welcome to the foliage season.");
			break;
		case November:
			JOptionPane.showMessageDialog(null, "Welcome to the foliage season.");
			break;
		case December:
			JOptionPane.showMessageDialog(null, "Do you want to build a snow man?");
			break;
		}
		select = (AccountType) JOptionPane.showInputDialog(null,"Select a month.", "List of months", JOptionPane.INFORMATION_MESSAGE, null, choices, choices[3]);
		}
		JOptionPane.showMessageDialog(null, "Good Bye");
	}
}