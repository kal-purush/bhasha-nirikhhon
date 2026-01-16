package com.project_merge.jigu_travel.api.location.controller;

import com.project_merge.jigu_travel.api.location.model.Place;
import com.project_merge.jigu_travel.api.location.service.LocationService;
import com.project_merge.jigu_travel.global.common.BaseResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@RestController
@RequestMapping("/location")
public class LocationController {

    // 사용자별 위치 데이터를 임시 저장
    private final Map<String, Map<String, Double>> userLocations = new HashMap<>();
    private final LocationService locationService;

    /**
     * 위치 데이터를 저장합니다.
     * @param userId 사용자 ID
     * @param locationData 위치 데이터 (latitude, longitude)
     * @return 저장 성공 메시지
     */
    @PostMapping("/user-location")
    public ResponseEntity<BaseResponse<String>> saveUserLocation(
            @RequestHeader(value = "Authorization", required = false) String userId,
            @RequestBody Map<String, Double> locationData) {
        if (userId == null || userId.isEmpty()) {
            userId = "anonymous"; // 사용자 ID가 없는 경우 "anonymous"로 설정
        }

        Double latitude = locationData.get("latitude");
        Double longitude = locationData.get("longitude");

        if (latitude == null || longitude == null) {
            return ResponseEntity.badRequest().body(new BaseResponse<>(400, "위치 데이터가 누락되었습니다.", null));
        }

        userLocations.put(userId, Map.of("latitude", latitude, "longitude", longitude));
        return ResponseEntity.ok(new BaseResponse<>(200, "위치 저장 성공", "Location saved for user: " + userId));
    }


    /**
     * 저장된 위치 데이터를 반환합니다.
     * @param userId 사용자 ID
     * @return 위치 데이터
     */
    @GetMapping("/user-location")
    public ResponseEntity<BaseResponse<Map<String, Double>>> getUserLocation(
            @RequestHeader(value = "Authorization", required = false) String userId) {
        if (userId == null || userId.isEmpty()) {
            userId = "anonymous";
        }

        if (!userLocations.containsKey(userId)) {
            return ResponseEntity.status(404).body(new BaseResponse<>(404, "위치 데이터가 없습니다.", null));
        }

        return ResponseEntity.ok(new BaseResponse<>(200, "위치 데이터 반환 성공", userLocations.get(userId)));
    }


    /**
     * 근처 명소 검색
     * @param latitude 사용자의 위도
     * @param longitude 사용자의 경도
     * @param radius 검색 반경 (km)
     * @return 근처 명소 리스트
     */
    @GetMapping("/nearby-places")
    public ResponseEntity<BaseResponse<List<Place>>> getNearbyPlaces(
            @RequestParam double latitude,
            @RequestParam double longitude,
            @RequestParam(defaultValue = "5.0") double radius) {
        List<Place> places = locationService.findNearbyPlaces(latitude, longitude, radius);
        return ResponseEntity.ok(new BaseResponse<>(200, "근처 명소 검색 성공", places));
    }

}