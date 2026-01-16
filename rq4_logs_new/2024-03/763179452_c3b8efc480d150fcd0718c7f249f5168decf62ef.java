package com.BackendChallenge.TechTrendEmporium.controller;

import com.BackendChallenge.TechTrendEmporium.controller.Requests.AddCouponRequest;
import com.BackendChallenge.TechTrendEmporium.controller.Requests.DeleteCouponRequest;
import com.BackendChallenge.TechTrendEmporium.entity.Coupon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.BackendChallenge.TechTrendEmporium.service.CouponService;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/admin/coupon")
public class CouponController {

    @Autowired
    private CouponService couponService;

    @PostMapping(value = "add")
    public ResponseEntity<?> addCoupon(@RequestBody AddCouponRequest request) {
        boolean coupon = couponService.addCoupon(request.getName(), request.getDiscount());
        if (coupon) {
            return ResponseEntity.created(null).body("Coupon added successfully");
        } else {
            return ResponseEntity.badRequest().body("Coupon already exists");
        }
    }

    @DeleteMapping(value = "delete")
    public ResponseEntity<?> deleteCoupon(@RequestBody DeleteCouponRequest request){
        boolean coupon = couponService.deleteCoupon(request.getName());
        if (coupon) {
            return ResponseEntity.ok("Coupon deleted successfully");
        } else {
            return ResponseEntity.badRequest().body("Coupon does not exist");
        }
    }

    @GetMapping(value = "all")
    public List<Coupon> getCoupons() {
        return couponService.getCoupons();
    }
}