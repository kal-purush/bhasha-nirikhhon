package pdes.unq.com.APC.dtos.mercadoLibre;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TokenResponse {
    String accessToken;
    String tokenType;
    int expiresIn;
    String scope;
    long userId;
    String refreshToken;
}