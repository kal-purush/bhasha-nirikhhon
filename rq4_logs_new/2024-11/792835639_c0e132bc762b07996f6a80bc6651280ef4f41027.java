package com.jinius.ecommerce.user.domain;

import com.jinius.ecommerce.common.EcommerceException;
import com.jinius.ecommerce.common.ErrorCode;
import com.jinius.ecommerce.user.api.UserChargeRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.math.BigInteger;

import static com.jinius.ecommerce.common.ErrorCode.INVALID_PARAMETER;
import static com.jinius.ecommerce.common.ErrorCode.NOT_FOUND_USER;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;

    /**
     * 유저 ID로 유저 조회
     * @param userId
     * @return
     */
    public User validateUserByUserId(Long userId) {
        if (userId <= 0) throw new EcommerceException(INVALID_PARAMETER);

        return userRepository.findById(userId).orElseThrow(() -> new EcommerceException(NOT_FOUND_USER));
    }

    /**
     * 주문금액과 유저 잔액 포인트 비교
     * @param user
     * @param orderPrice
     */
    public void comparePoint(User user, BigInteger orderPrice) {
        if (ObjectUtils.isEmpty(user)) throw new EcommerceException(INVALID_PARAMETER);
        if (orderPrice.compareTo(BigInteger.ZERO) <= 0) throw new EcommerceException(INVALID_PARAMETER);

        user.setPoint(BigInteger.valueOf(50000));
        if (user.getPoint().compareTo(orderPrice) < 0)
            throw new EcommerceException(ErrorCode.NOT_ENOUGH_POINT);
    }

    /**
     * 포인트 충전
     * @param request
     * @return
     */
    public User chargePoint(UserChargeRequest request) {
        if (ObjectUtils.isEmpty(request)) throw new EcommerceException(INVALID_PARAMETER);
        if (request.getChargePoint().compareTo(BigInteger.ZERO) <= 0) throw new EcommerceException(INVALID_PARAMETER);

        User user = validateUserByUserId(request.getUserId());
        user.addPoint(request.getChargePoint());
        System.out.println("user.getPoint() = " + user.getPoint());

        return userRepository.save(user);


    }

}