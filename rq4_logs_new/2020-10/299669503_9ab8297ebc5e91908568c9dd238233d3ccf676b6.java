import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class AddressBookService {
	public static String PAYROLL_FILE_NAME = "AddressBook.txt";

	public void writeData(Map<String, AddressBook> addressBookMap) {
		StringBuffer employeeBuffer = new StringBuffer();
		for(Map.Entry<String, AddressBook> entry : addressBookMap.entrySet()) {
			entry.getValue().getBook().forEach(contact -> {
			String empString = contact.toString().concat("\n");
			employeeBuffer.append(empString);
		});
		}
		try {
			Files.write(Paths.get(PAYROLL_FILE_NAME), employeeBuffer.toString().getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void readData() {
		try {
			Files.lines(new File(PAYROLL_FILE_NAME).toPath()).forEach(System.out::println);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}