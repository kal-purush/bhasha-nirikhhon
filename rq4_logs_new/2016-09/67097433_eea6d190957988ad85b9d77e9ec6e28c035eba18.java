package com.pizza5stars.authentication;

import com.github.toastshaman.dropwizard.auth.jwt.JsonWebTokenValidator;
import com.github.toastshaman.dropwizard.auth.jwt.model.JsonWebToken;
import com.github.toastshaman.dropwizard.auth.jwt.validator.ExpiryValidator;
import com.google.common.base.Optional;
import com.pizza5stars.dao.CustomerDAO;
import com.pizza5stars.representations.Customer;
import io.dropwizard.auth.Authenticator;
import org.skife.jdbi.v2.DBI;

import java.security.Principal;

public class JWTAuthenticator implements Authenticator<JsonWebToken, Principal> {
    private final CustomerDAO customerDAO;

    public JWTAuthenticator(DBI jdbi) {
        customerDAO = jdbi.onDemand(CustomerDAO.class);
    }

    @Override
    public Optional<Principal> authenticate(JsonWebToken token) {
        final JsonWebTokenValidator expiryValidator = new ExpiryValidator();

        // Provide your own implementation to lookup users based on the principal attribute in the
        // JWT Token. E.g.: lookup users from a database etc.
        // This method will be called once the token's signature has been verified

        // In case you want to verify different parts of the token you can do that here.
        // E.g.: Verifying that the provided token has not expired.

        // All JsonWebTokenExceptions will result in a 401 Unauthorized response.

        expiryValidator.validate(token);

        int id = (int) token.claim().getParameter("id");
        String email = (String) token.claim().getParameter("email");

        Customer user = customerDAO.getCustomerById(id);

        if (user != null) {
            if (user.getEmail().equals(email) && user.getId() == id) {
                final Customer principal = new Customer(id, user.getEmail(), user.getFirstName(), user.getLastName(), "");
                return Optional.of(principal);
            }
        }

        return Optional.absent();
    }
}
