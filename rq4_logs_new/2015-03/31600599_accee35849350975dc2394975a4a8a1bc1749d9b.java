package org.screwdriver.idm.service;

import org.screwdriver.idm.dto.AccountDTO;

public interface ITokenService {

    /**
     *
     * @param account DTO with account data to be bundled into token
     * @return Base64 encoded token String containing account and expiration data with mac
     * @throws Exception
     */
    public String generateToken(AccountDTO account) throws Exception;

    /**
     *
     * @param token Authentication token to be validated
     * @throws Exception
     */
    public boolean validate(String token) throws Exception;



}