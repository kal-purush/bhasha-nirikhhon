package transport4future.tokenManagement.controller;

import transport4future.tokenManagement.exception.LMException;
import transport4future.tokenManagement.model.Token;
import transport4future.tokenManagement.service.Crypt;
import transport4future.tokenManagement.service.TokenStorage;
import transport4future.tokenManagement.model.implementation.TokenManagerInterface;

import java.time.LocalDateTime;

//TODO: volver a reciuperarla
public class TokenManager implements TokenManagerInterface {
    Crypt crypt = new Crypt();
    TokenStorage tokenStorage = new TokenStorage();
    @Override
    public boolean VerifyToken(String token) throws LMException {
        /**
         * Decode token
         */
        Token decodedToken;
        try {
            decodedToken = this.crypt.decode(token);
        } catch(Exception e) {
            throw new LMException("La cadena de caracteres de la entrada no se corresponde con un token que se pueda procesar.");
        }

        /**
         * Check token expiration
         */
        if(decodedToken.getPayload().getExpirationDate().isBefore(LocalDateTime.now())) {
            throw new LMException("Token expirado.");
        }

        /**
         * Check token validity
         */
        if(!this.tokenStorage.has(decodedToken)) {
            throw new LMException("No se encuentra registrado el token para el cual se solicita verificación.");
        }
        return true; // At this point it will be always true, either an exception would've been thrown already...
    }
}