package fa.training.auth;


import fa.training.entities.User;
import fa.training.repositories.UsersRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class CustomUserDetailService implements UserDetailsService {

    @Autowired
    UsersRepository usersRepository;


    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {

        Optional<User> user = Optional.ofNullable(usersRepository.findByUserName(username));

        if (user.isEmpty()) {
            throw new UsernameNotFoundException("User not found");
        }

        User userDB = user.get();

        return new CustomUserDetail(userDB);
    }
}