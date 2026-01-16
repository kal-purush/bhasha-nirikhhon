package gigjob.controller;

import gigjob.model.response.ShopResponse;
import gigjob.service.ShopService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.modelmapper.ModelMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Log4j2
@RestController
@RequestMapping(value = "/api")
@RequiredArgsConstructor
public class ShopController {
    private final ModelMapper modelMapper;
    private final ShopService shopService;

    @GetMapping("/v1/shop")
    public ResponseEntity<List<ShopResponse>> getShopList() {
        List<ShopResponse> shopResponseList = shopService.getShopList();
        return ResponseEntity.status(HttpStatus.OK).body(shopResponseList);
    }
}