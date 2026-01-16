package pe.edu.ceid.simi.operative.application.authentication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pe.edu.ceid.simi.operative.domain.authentication.model.Authentication;
import pe.edu.ceid.simi.operative.domain.authentication.repository.AuthenticationRepository;

@Service
public class AuthenticationServiceImpl implements AuthenticationService{

	@Autowired
	private AuthenticationRepository repository;

	@Override
	public Authentication signInWithEmailAndPassword(String email, String password) {
		return this.repository.signInWithEmailAndPassword(email, password);
	}

	@Override
	public Authentication signInInvited() {
		return this.repository.signInInvited();
	}

}