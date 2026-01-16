package back;

public class StoreAndRestore {
	private static double backResult;

	public static double getBackResult() {
		return backResult;
	}

	public static void setBackResult(double backResult) {
		StoreAndRestore.backResult = backResult;
	}

}