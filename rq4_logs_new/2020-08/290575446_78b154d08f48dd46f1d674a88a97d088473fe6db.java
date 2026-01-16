package BootStrapperPackage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;

import CustomizedExceptionsPackage.ImportantDetailsMissingException;
import TableOperationPackage.ShiftOperations;

public class BootStrapperClass {

	public static void main(String[] args) throws FileNotFoundException, IOException, ImportantDetailsMissingException,
			ClassNotFoundException, SQLException {

		BootStrappingOperations.getBootStrappingOperations().loadDBConnectionDataFromDBConfigurationFile();
		BootStrappingOperations.getBootStrappingOperations().registerDBDriver();
		BootStrappingOperations.getBootStrappingOperations().connectLoadTableDataAndDisconnectFromDB();
		testMyCode();
	}

	private static void testMyCode() throws SQLException, IOException {
		// scan non null values of parent and child through System.in
		// input strings will be like this "CHILD PARENT"
		// although i have handled those test cases;

		BufferedReader bufferedReader = null;
		System.out.println("Enter shifCode {space} shiftGroupingCode");
		try {
			bufferedReader = new BufferedReader(new InputStreamReader(System.in));

			String[] inputArray = bufferedReader.readLine().split(" ");

			String childInput = null;
			String parentInput = null;

			if (inputArray.length != 2) {
				System.out.println("2 Strings are not given as input");
				return;
			}

			childInput = inputArray[0].trim();
			parentInput = inputArray[1].trim();

			if (childInput == null || parentInput == null || childInput.isEmpty() || parentInput.isEmpty()) {
				System.out.println("valid Strings are not given");
				return;
			}

			System.out.println(
					ShiftOperations.getShiftOperations().doesShiftbelongToGroupHierarchy(childInput, parentInput));

		} finally {
			bufferedReader.close();
		}
	}

}