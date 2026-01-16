import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
public class HashCodeGenerator {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		BCryptPasswordEncoder e=new BCryptPasswordEncoder();
		String h=e.encode("ULPCJHYNB");
		
		//System.out.println(BCrypt.checkpw("OOTTGEGLA", h));
		System.out.println(h);
	}

}