import com.lj.mybatis.entity.User;
import com.lj.mybatis.service.impl.UserServiceImpl;


public class UserTest {
	public static void main(String[] args) {
		int id=1;
		UserServiceImpl service=new UserServiceImpl();
		User user=service.selectUserByID(id);
		System.out.println(user.getUserName());
		
		
	}

}