package uk.ac.bangor.cs.cambria.AcademiGymraeg.util;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.model.User;
import uk.ac.bangor.cs.cambria.AcademiGymraeg.repo.UserRepository;

/**
 * @author grs22lkc
 */

// Create service to identify which user is currently logged in
@Service
public class UserService {

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    // Get the logged in user's ID
    public Long getLoggedInUserId() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null && authentication.isAuthenticated()) {
            String username = authentication.getName();
            User user = userRepository.findByEmailAddress(username).orElse(null);

            if (user != null) {
                return user.getUserId();
            }
        }

        return null; // Return null if no user is logged in
    }

    // Get the logged in user's forename
    public String getLoggedInUserForename() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null && authentication.isAuthenticated()) {
            String username = authentication.getName();
            User user = userRepository.findByEmailAddress(username).orElse(null);

            if (user != null) {
                return user.getForename();
            }
        }

        return "Unknown User"; // Default value if no user is logged in
    }

    // Get the logged in user's email address
    public String getLoggedInUserEmail() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null && authentication.isAuthenticated()) {
            String username = authentication.getName();
            User user = userRepository.findByEmailAddress(username).orElse(null);

            if (user != null) {
                return user.getUsername();
            }
        }

        return null; // Return null if no user is logged in
    }

    // Check if the logged in user is an administrator
    public boolean isLoggedInUserAdmin() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null && authentication.isAuthenticated()) {
            String username = authentication.getName();
            User user = userRepository.findByEmailAddress(username).orElse(null);

            if (user != null) {
                return user.isAdmin();
            }
        }

        return false; // Default to false if no user is logged in
    }

    // Check if the logged in user is an instructor
    public boolean isLoggedInUserInstructor() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication != null && authentication.isAuthenticated()) {
            String username = authentication.getName();
            User user = userRepository.findByEmailAddress(username).orElse(null);

            if (user != null) {
                return user.isInstructor();
            }
        }

        return false; // Default to false if no user is logged in
    }
}