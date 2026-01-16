package com.group7.mezat.controllers;

import com.group7.mezat.requests.PackageSoldRequest;
import com.group7.mezat.requests.PackageUpdateRequest;
import com.group7.mezat.services.PackageService;
import com.group7.mezat.documents.FishPackage;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/package")
@AllArgsConstructor
public class PackageController {

    private PackageService packageService;

    @GetMapping("/{buyerId}")
    public List<FishPackage> getOneUsersPackages(@PathVariable String buyerId){
        return packageService.getOneUsersPackages(buyerId);
    }

    @PostMapping
    public void addPackage(@RequestBody FishPackage fishPackage){
        packageService.addPackage(fishPackage);
    }

    @GetMapping("/allSoldPackages")
    public List<FishPackage> getAllSoldPackages(){
        return packageService.getAllSoldPackages();
    }

    @GetMapping("/allUnsoldPackages")
    public List<FishPackage> getAllUnSoldPackages(){
        return packageService.getAllUnsoldPackages();
    }

    @DeleteMapping("/delete/{packageId}")
    public void deletePackage(@PathVariable String packageId){
        packageService.deletePackage(packageId);
    }

    @PutMapping("/update/{packageId}")
    public void updatePackage(@PathVariable String packageId, @RequestBody PackageUpdateRequest updateRequest){
        packageService.updatePackage(packageId, updateRequest);
    }

    @PutMapping("/sell/{packageId}")
    public void sellPackage(@PathVariable String packageId, @RequestBody PackageSoldRequest soldRequest){
        packageService.sellPackage(packageId, soldRequest);
    }


}