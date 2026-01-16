package com.it.academy.mortgage.calculator.mappers;

import com.it.academy.mortgage.calculator.dto.CalculateFormDto;
import com.it.academy.mortgage.calculator.dto.CalculateResultsDto;
import com.it.academy.mortgage.calculator.models.CalculateForm;
import com.it.academy.mortgage.calculator.models.CalculateResults;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class CalculateMapper {

    public CalculateForm fromFormDto (CalculateFormDto calculateFormDto) {
        if (calculateFormDto == null) {
            return null;
        }

        CalculateForm calculateForm = new CalculateForm();

        calculateForm.setId(calculateFormDto.getId());
        calculateForm.setHomePrice(calculateFormDto.getHomePrice());
        calculateForm.setMonthlyFamilyIncome(calculateFormDto.getMonthlyFamilyIncome());
        calculateForm.setLoanTerm(calculateFormDto.getLoanTerm());
        calculateForm.setFamilyMembers(calculateFormDto.getFamilyMembers());
        calculateForm.setHaveChildren(calculateFormDto.isHaveChildren());
        calculateForm.setCity(calculateFormDto.getCity());

        calculateForm.setCalculateResults(fromResultsDto(calculateFormDto.getCalculateResultsDto()));

        return calculateForm;
    }

    public CalculateFormDto toFormDto (CalculateForm calculateForm){
        if (calculateForm == null) {
            return null;
        }

        CalculateFormDto dto = new CalculateFormDto();
        dto.setId(calculateForm.getId());
        dto.setHomePrice(calculateForm.getHomePrice());
        dto.setMonthlyFamilyIncome(calculateForm.getMonthlyFamilyIncome());
        dto.setLoanTerm(calculateForm.getLoanTerm());
        dto.setFamilyMembers(calculateForm.getFamilyMembers());
        dto.setHaveChildren(calculateForm.isHaveChildren());
        dto.setCity(calculateForm.getCity());

        dto.setCalculateResultsDto(toResultsDto(calculateForm.getCalculateResults()));

        return dto;
    }

    public CalculateResults fromResultsDto (CalculateResultsDto calculateResultsDto){
        if (calculateResultsDto == null) {
            return null;
        }

        CalculateResults calculateResults = new CalculateResults();

        calculateResults.setId(calculateResultsDto.getId());
        calculateResults.setMaxLoan(calculateResultsDto.getMaxLoan());
        calculateResults.setTotalInterestPaid(calculateResultsDto.getTotalInterestPaid());
        calculateResults.setAgreementFee(calculateResultsDto.getAgreementFee());
        calculateResults.setTotalPaymentSum(calculateResultsDto.getTotalPaymentSum());

        return calculateResults;
    }

    public CalculateResultsDto toResultsDto (CalculateResults calculateResults){
        if (calculateResults == null) {
            return null;
        }

        CalculateResultsDto dto = new CalculateResultsDto();
        dto.setId(calculateResults.getId());
        dto.setMaxLoan(calculateResults.getMaxLoan());
        dto.setTotalInterestPaid(calculateResults.getTotalInterestPaid());
        dto.setAgreementFee(calculateResults.getAgreementFee());
        dto.setTotalPaymentSum(calculateResults.getTotalPaymentSum());

        return dto;
    }

    public List<CalculateResultsDto> toResultsDtoList (List<CalculateResults> entities){
        List<CalculateResultsDto> dtos = new ArrayList<>();

        for (CalculateResults entity : entities){
            dtos.add(toResultsDto(entity));
        }
        return dtos;
    }

    public List<CalculateFormDto> toDtoList (List<CalculateForm> entities){
        List<CalculateFormDto> dtos = new ArrayList<>();

        for (CalculateForm entity : entities){
            dtos.add(toFormDto(entity));
        }
        return dtos;
    }
}