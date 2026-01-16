package leets.weeth.domain.user.domain.entity;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public record SecurityUser(
        Long id,
        String email,
        String name,
        String role,
        boolean active
) implements UserDetails, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public static SecurityUser from(User u) {
        return new SecurityUser(
                u.getId(),
                u.getEmail(),
                u.getName(),
                u.getRole().name(),
                !u.isInactive()
        );
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return List.of(new SimpleGrantedAuthority("ROLE_" + role));
    }

    @Override
    public String getPassword() {
        return "N/A";
    }

    @Override
    public String getUsername() {
        return name;
    }

    @Override
    public boolean isAccountNonExpired() {
        return active;
    }

    @Override
    public boolean isAccountNonLocked() {
        return active;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return active;
    }
}