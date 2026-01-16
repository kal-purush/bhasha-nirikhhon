package com.bin448.backend.converter;

import com.bin448.backend.entity.DTOentity.RentACarBranchOfficeDTO;
import com.bin448.backend.entity.RentACarBranchOffice;
import com.bin448.backend.repository.CarServiceRepository;
import com.bin448.backend.repository.RentACarBrenchOfficeRespository;

public class RentACarBrenchOfficeConverter extends AbstractConverter{
    private static CarServiceRepository cr;
    private static RentACarBrenchOfficeRespository rentACarBrenchOfficeRespository;
    public RentACarBrenchOfficeConverter(CarServiceRepository cr,RentACarBrenchOfficeRespository rentACarBrenchOfficeRespository){
        this.cr = cr;
        this.rentACarBrenchOfficeRespository = rentACarBrenchOfficeRespository;
    }
    public static RentACarBranchOfficeDTO fromEntity(RentACarBranchOffice office){
        RentACarBranchOfficeDTO dto = new RentACarBranchOfficeDTO();
        dto.setAddress(office.getAddress());
        dto.setId(office.getId());
        dto.setServiceName(office.getCarService().getCarServiceName());
        return  dto;
    }

    public static  RentACarBranchOffice toEntity(RentACarBranchOfficeDTO dto){
        RentACarBranchOffice office = new RentACarBranchOffice();
        office.setAddress(dto.getAddress());
        office.setId(dto.getId());
        office.setCarService(cr.getCarServiceByCarServiceName(dto.getServiceName()));
        return office;
    }
}