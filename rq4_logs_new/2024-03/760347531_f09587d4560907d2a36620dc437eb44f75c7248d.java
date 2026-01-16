package lk.zerocode.api.controller;

import jakarta.validation.Valid;
import lk.zerocode.api.controller.dto.BasicDetailsDTO;
import lk.zerocode.api.exceptions.EmployeeNotFoundException;
import lk.zerocode.api.model.Employee;
import lk.zerocode.api.service.BasicDetailsWithDto;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@AllArgsConstructor
public class BasicDetailControllerWithDto {

    private BasicDetailsWithDto basicDetailsWithDto;

    @PostMapping("/employees")
    public ResponseEntity<BasicDetailsDTO> saveBasicDetails(@RequestBody @Valid BasicDetailsDTO basicDetailsDTO){
        BasicDetailsDTO createdemployee = basicDetailsWithDto.saveBasicDetails(basicDetailsDTO);
        return new ResponseEntity<>(createdemployee, HttpStatus.CREATED);
    }

    @GetMapping("/allemployees")
    public ResponseEntity<List<BasicDetailsDTO>> getAllDetails(){
        List<BasicDetailsDTO> allDetails = basicDetailsWithDto.getAllDetails();
        return new ResponseEntity<>(allDetails, HttpStatus.OK);
    }


    @GetMapping("/empname")
    public ResponseEntity<List<Employee>> getByName(@RequestBody BasicDetailsDTO basicDetailsDTO) {
        String fullName = basicDetailsDTO.getFirstName();
        List<Employee> details = basicDetailsWithDto.getByEmpEmail(basicDetailsDTO);
        return new ResponseEntity<>(details, HttpStatus.OK);
    }
}