package com.upgrad.FoodOrderingApp.service.businness;

import com.upgrad.FoodOrderingApp.service.common.ApplicationUtil;
import org.springframework.stereotype.Service;
import com.upgrad.FoodOrderingApp.service.dao.CustomerAuthDao;
import com.upgrad.FoodOrderingApp.service.dao.CustomerDao;
import com.upgrad.FoodOrderingApp.service.entity.CustomerAuthEntity;
import com.upgrad.FoodOrderingApp.service.entity.CustomerEntity;
import com.upgrad.FoodOrderingApp.service.exception.AuthenticationFailedException;
import com.upgrad.FoodOrderingApp.service.exception.AuthorizationFailedException;
import com.upgrad.FoodOrderingApp.service.exception.SignUpRestrictedException;
import com.upgrad.FoodOrderingApp.service.exception.UpdateCustomerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * @author zeelani
 * Service class to handler the customer business logic operations
 */

@Service
public class CustomerService {
    @Autowired
    CustomerDao customerDao;

    @Autowired
    PasswordCryptographyProvider passwordCryptographyProvider;

    @Autowired
    ApplicationUtil applicationUtil;

    @Autowired
    CustomerAuthDao customerAuthDao;


    /**
     * Method to save the customer.
     * Validates the entity data prior to DB operation
     * @param customerEntity
     * @return
     * @throws SignUpRestrictedException
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public CustomerEntity saveCustomer(CustomerEntity customerEntity) throws SignUpRestrictedException {
        //checking whether the customer entry already exists
        CustomerEntity lookUpEntity = customerDao.getCustomerByContactNumber(customerEntity.getContactNumber());
        if (lookUpEntity != null) {
            throw new SignUpRestrictedException("SGR-001", "This contact number is already registered! Try other contact number");
        }
        //validating the fields entered
        if (!applicationUtil.isValidSignupRequest(customerEntity)) {
            throw new SignUpRestrictedException("SGR-005", "Except last name all fields should be filled");
        }
        //validating the email format
        if (!applicationUtil.isEmailValid(customerEntity.getEmail())) {
            throw new SignUpRestrictedException("SGR-002", "Invalid email-id format!");
        }
        //validating the contact number
        if (!applicationUtil.isContactValid(customerEntity.getContactNumber())) {
            throw new SignUpRestrictedException("SGR-003", "Invalid contact number!");
        }
        //verifying the strength of the password
        if (!applicationUtil.isValidPassword(customerEntity.getPassword())) {
            throw new SignUpRestrictedException("SGR-004", "Weak password!");
        }

        //Encoding the password using the cryptography provider
        String[] encryptedPassword = passwordCryptographyProvider.encrypt(customerEntity.getPassword());
        customerEntity.setSalt(encryptedPassword[0]);
        customerEntity.setPassword(encryptedPassword[1]);

        //all good to safestore the customer record in the database
        CustomerEntity newCustomerEntity = customerDao.createCustomer(customerEntity);

        return newCustomerEntity;
    }

    /**
     * Method to authenticate the customer using the password
     * @param contactNumber
     * @param password
     * @return
     * @throws AuthenticationFailedException
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public CustomerAuthEntity authenticate(String contactNumber, String password) throws AuthenticationFailedException {
        CustomerEntity customerEntity = customerDao.getCustomerByContactNumber(contactNumber);
        if (customerEntity == null) {
            throw new AuthenticationFailedException("ATH-001", "This contact number has not been registered!");
        }
        String encryptedPassword = passwordCryptographyProvider.encrypt(password, customerEntity.getSalt());
        if (encryptedPassword.equals(customerEntity.getPassword())) {
            JwtTokenProvider jwtTokenProvider = new JwtTokenProvider(encryptedPassword);
            CustomerAuthEntity customerAuthEntity = new CustomerAuthEntity();
            customerAuthEntity.setCustomer(customerEntity);
            final ZonedDateTime now = ZonedDateTime.now();
            final ZonedDateTime expiresAt = now.plusHours(8);
            customerAuthEntity.setAccessToken(jwtTokenProvider.generateToken(customerEntity.getUuid(), now, expiresAt));
            customerAuthEntity.setLoginAt(now);
            customerAuthEntity.setExpiresAt(expiresAt);
            customerAuthEntity.setUuid(UUID.randomUUID().toString());
            // creating the customer auth entity
            CustomerAuthEntity createdCustomerAuthEntity = customerAuthDao.createCustomerAuth(customerAuthEntity);
            return createdCustomerAuthEntity;
        } else {
            // invalid credentials throw exception
            throw new AuthenticationFailedException("ATH-002", "Invalid Credentials");
        }

    }

    /**
     * Logout from the app
     * @param accessToken
     * @return
     * @throws AuthorizationFailedException
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public CustomerAuthEntity logout(String accessToken) throws AuthorizationFailedException {
        CustomerAuthEntity customerAuthEntity = getCustomerAuthEntity(accessToken);
        customerAuthEntity.setLogoutAt(ZonedDateTime.now());
        CustomerAuthEntity upatedCustomerAuthEntity = customerAuthDao.customerLogout(customerAuthEntity);
        return upatedCustomerAuthEntity;
    }

    /**
     * Method to update the customer Entity
     * @param customerEntity
     * @return
     * @throws UpdateCustomerException
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public CustomerEntity updateCustomer(CustomerEntity customerEntity) throws UpdateCustomerException {
        CustomerEntity customerToBeUpdated = customerDao.getCustomerByUuid(customerEntity.getUuid());
        customerToBeUpdated.setFirstName(customerEntity.getFirstName());
        customerToBeUpdated.setLastName(customerEntity.getLastName());
        CustomerEntity updatedCustomer = customerDao.updateCustomer(customerEntity);
        return updatedCustomer;
    }


    /**
     * Method to update the customer entity
     * @param oldPassword
     * @param newPassword
     * @param customerEntity
     * @return
     * @throws UpdateCustomerException
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public CustomerEntity updateCustomerPassword(String oldPassword,String newPassword,CustomerEntity customerEntity ) throws UpdateCustomerException {
        if (!applicationUtil.isValidPassword(newPassword)) {
            throw new UpdateCustomerException("UCR-001", "Weak password!");
        }
        String encryptedOldPassword = passwordCryptographyProvider.encrypt(oldPassword, customerEntity.getSalt());
        if (encryptedOldPassword.equals(customerEntity.getPassword())) {
            CustomerEntity tobeUpdatedCustomerEntity = customerDao.getCustomerByUuid(customerEntity.getUuid());
            String[] encryptedPassword = passwordCryptographyProvider.encrypt(newPassword);
            tobeUpdatedCustomerEntity.setSalt(encryptedPassword[0]);
            tobeUpdatedCustomerEntity.setPassword(encryptedPassword[1]);
            CustomerEntity updatedCustomerEntity = customerDao.updateCustomer(tobeUpdatedCustomerEntity);
            return updatedCustomerEntity;
        } else {
            throw new UpdateCustomerException("UCR-004", "Incorrect old password!");
        }
    }

    /**
     * Get the customer entity using the access token
     * @param accessToken
     * @return
     * @throws AuthorizationFailedException
     */
    public CustomerEntity getCustomer(String accessToken) throws AuthorizationFailedException {
        CustomerAuthEntity customerAuthEntity = getCustomerAuthEntity(accessToken);
        return customerAuthEntity.getCustomer();
    }

    /**
     * A private method to get the customer auth entity
     * @param accessToken
     * @return
     * @throws AuthorizationFailedException
     */
    private CustomerAuthEntity getCustomerAuthEntity(String accessToken) throws AuthorizationFailedException {
        CustomerAuthEntity customerAuthEntity = customerAuthDao.getCustomerAuthByAccessToken(accessToken);
        if (customerAuthEntity == null) {
            throw new AuthorizationFailedException("ATHR-001", "Customer is not Logged in.");
        }
        if (customerAuthEntity.getLogoutAt() != null) {
            throw new AuthorizationFailedException("ATHR-002", "Customer is logged out. Log in again to access this endpoint.");
        }
        final ZonedDateTime now = ZonedDateTime.now();
        if (customerAuthEntity.getExpiresAt().compareTo(now) < 0) {
            throw new AuthorizationFailedException("ATHR-003", "Your session is expired. Log in again to access this endpoint.");
        }
        return customerAuthEntity;
    }
}