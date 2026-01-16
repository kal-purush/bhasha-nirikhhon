package shop.kokodo.calculateservice.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import shop.kokodo.calculateservice.dto.ardto.CouponInfo;
import shop.kokodo.calculateservice.dto.response.Response;
import shop.kokodo.calculateservice.entity.ArCoupon;
import shop.kokodo.calculateservice.service.ArCouponService;

/**
 * packageName    : shop.kokodo.calculateservice.controller
 * fileName       : ArCouponController
 * author         : namhyeop
 * date           : 2022/11/02
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2022/11/02        namhyeop       최초 생성
 */

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/arCoupon")
public class ArCouponController {

    private final ArCouponService arCouponService;

    @PostMapping("/saveCoupon")
    public void saveCoupon(@RequestBody CouponInfo couponInfo){
        arCouponService.createCoupon(couponInfo);
    }

    @GetMapping("/searchCoupon/{sellerId}/{productId}")
    public Response getCoupon(@PathVariable Long sellerId, @PathVariable Long productId){
       return Response.success(arCouponService.findArCouponList(sellerId, productId));
    }
}