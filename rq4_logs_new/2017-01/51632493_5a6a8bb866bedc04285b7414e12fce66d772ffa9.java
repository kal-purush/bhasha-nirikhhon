/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareok.data.kernel.api.services.user;

/**
 *
 * @author Tao Zhao
 */
public interface PasswordAuthenticationService {
    public void savePasswordHash(long userId);
    public String hash(String password);
    public String getPasswordHash(long userId);
    public boolean authenticate(String password, String token);
}