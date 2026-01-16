package org.mota.carselectionservice.controller;

import lombok.RequiredArgsConstructor;
import org.mota.carselectionservice.integration.CarProviderClient;
import org.mota.grpc.CarFilter;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cars")
@RequiredArgsConstructor
public class CarsController {

  private final CarProviderClient carProviderClient;

  @PostMapping("/find-new")
  public void findNewCars(@RequestBody(required = false) CarFilter carFilter) {
    carProviderClient.findCarsByFilter(carFilter);
  }

}