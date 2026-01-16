package Lesson5and6;

import java.util.Map;

public  class UserAuthority implements Authority{
	View view;

	public UserAuthority(View view) {
		this.view = view;
	}

	@Override
	public boolean check(String login, String password, Map<String, String> usersDB) {

		boolean bool = false;

		if(!usersDB.containsKey(login)){
			view.print("Пользователь не найден");
			return false;
		}

		for(String user : usersDB.keySet()){
			if(user.equals(login)){
				if(password.equals(usersDB.get(login))){
					view.print("Авторизация успешна");
					bool =  true;
				} else {
					view.print("Ошибка авторизации");
					bool = false;
				}
			}
		}
		return bool;
	}
}