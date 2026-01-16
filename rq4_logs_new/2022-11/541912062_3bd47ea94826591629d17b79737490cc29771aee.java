package shop.kokodo.calculateservice.circuitbreaker;

import feign.Response;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import static shop.kokodo.calculateservice.exception.message.BatchErrorMessage.ORDER_FEIGN_FAIL;
import static shop.kokodo.calculateservice.exception.message.BatchErrorMessage.PRODUCT_FEIGN_FAIL;

/**
 * packageName    : shop.kokodo.calculateservice.circuitbreaker
 * fileName       : ErrorDecoder
 * author         : namhyeop
 * date           : 2022/11/17
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2022/11/17        namhyeop       최초 생성
 */
@Component
public class ErrorDecoder implements feign.codec.ErrorDecoder {

    @Override
    public Exception decode(String methodKey, Response response) {
        switch(response.status()) {
            case 400:
                if(methodKey.contains("getProductSellerId")) {
                    return new ResponseStatusException(HttpStatus.valueOf(response.status()), PRODUCT_FEIGN_FAIL);
                }
                if(methodKey.contains("searchCommissionPolicy")) {
                    return new ResponseStatusException(HttpStatus.valueOf(response.status()), ORDER_FEIGN_FAIL);
                }
                if(methodKey.contains("getSellerFinanceInfo")) {
                    return new ResponseStatusException(HttpStatus.valueOf(response.status()), ORDER_FEIGN_FAIL);
                }
                break;
            case 404:
                if(methodKey.contains("getProductSellerId")) {
                    return new ResponseStatusException(HttpStatus.valueOf(response.status()), PRODUCT_FEIGN_FAIL);
                }
                if(methodKey.contains("searchCommissionPolicy")) {
                    return new ResponseStatusException(HttpStatus.valueOf(response.status()), ORDER_FEIGN_FAIL);
                }
                if(methodKey.contains("getSellerFinanceInfo")) {
                    return new ResponseStatusException(HttpStatus.valueOf(response.status()), ORDER_FEIGN_FAIL);
                }
                break;
        }
        return null;
    }
}