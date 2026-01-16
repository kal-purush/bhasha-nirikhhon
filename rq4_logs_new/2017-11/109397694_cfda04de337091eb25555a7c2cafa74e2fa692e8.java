import com.dataprovider.DataProviderFactory;
import com.dataprovider.inf.DataProvider;

public class DataProviderTest {

	public static void main(String[] args) {
		
		DataProviderFactory factory = new DataProviderFactory();
		DataProvider dataProvider = factory.getProvider();
		System.out.println(dataProvider.toString());
	}
	
}