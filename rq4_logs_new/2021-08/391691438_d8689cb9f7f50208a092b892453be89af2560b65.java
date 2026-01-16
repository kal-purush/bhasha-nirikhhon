import business.concretes.MailVerificationManager;
import business.concretes.UserCheckManager;
import business.concretes.UserManager;
import dataAccess.concretes.UserDal;
import entities.concretes.User;

public class Main {

	public static void main(String[] args) {
		User user1 = new User();
		user1.setId(0);
		user1.setFirstName("Halil");
		user1.setLastName("Kurt");
		user1.setEmail("halil@gmail.com");
		user1.setPassword("12345646565");
		
		
		User user2 = new User();
		user2.setId(1);
		user2.setFirstName("Halil");
		user2.setLastName("Kurt");
		user2.setEmail("test@gmail.com");
		user2.setPassword("12345646565");
		
		
		UserManager userManager = new UserManager(new UserCheckManager(), new MailVerificationManager(), new UserDal());
		userManager.singUp(user1);
		userManager.singUp(user2);

		
	}

}