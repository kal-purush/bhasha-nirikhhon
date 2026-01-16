package by.bsuir.tattoo4u.controller;

import by.bsuir.tattoo4u.dto.request.RegistrationMasterRequestDto;
import by.bsuir.tattoo4u.dto.request.UsernameRequestDto;
import by.bsuir.tattoo4u.dto.response.MasterResponseDto;
import by.bsuir.tattoo4u.dto.response.UserResponseDto;
import by.bsuir.tattoo4u.entity.Master;
import by.bsuir.tattoo4u.entity.RoleType;
import by.bsuir.tattoo4u.entity.User;
import by.bsuir.tattoo4u.repository.MasterRepository;
import by.bsuir.tattoo4u.service.MasterService;
import by.bsuir.tattoo4u.service.ServiceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("api/masters")
public class MasterController {

    private MasterService masterService;

    @Autowired
    public MasterController(MasterService masterService) {
        this.masterService = masterService;
    }

    @GetMapping
    @PreAuthorize("isAuthenticated()")
    public ResponseEntity<?> getAllMasters(@PageableDefault(sort = "rating", direction = Sort.Direction.DESC) Pageable pageable) {

        List<Master> masterList=masterService.getAll(pageable);

        List<MasterResponseDto> masterResponseDto=new ArrayList<>();
        for (Master master: masterList){
            masterResponseDto.add(new MasterResponseDto(master));
        }

        return new ResponseEntity<>(masterResponseDto, HttpStatus.OK);
    }

    @PostMapping("/getByUsername")
    @PreAuthorize("isAuthenticated()")
    public ResponseEntity<?> getMasterByUsername(@PageableDefault(sort = "rating", direction = Sort.Direction.DESC) Pageable pageable, @RequestBody @Validated UsernameRequestDto requestDto){

        List<Master> masterList=masterService.getAllUsernameContain(requestDto.getUsername(), pageable);

        List<MasterResponseDto> masterResponseDto=new ArrayList<>();
        for (Master master: masterList){
            masterResponseDto.add(new MasterResponseDto(master));
        }

        return new ResponseEntity<>(masterResponseDto, HttpStatus.OK);
    }

    @GetMapping("{id}")
    @PreAuthorize("isAuthenticated()")
    public ResponseEntity<?> getMasterById(@PathVariable("id") User user){

        if(user==null){
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        MasterResponseDto masterResponseDto=null;
        if(user.getRoles().get(0).getName().equals(RoleType.MASTER.name())) {
            masterResponseDto = new MasterResponseDto(user.getMasterInfo());
        }else{
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        return new ResponseEntity<>(masterResponseDto, HttpStatus.OK);
    }
}