package shop.kokodo.calculateservice.dto;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * packageName    : shop.kokodo.calculateservice.dto
 * fileName       : AnnualSaleDto
 * author         : namhyeop
 * date           : 2022/11/11
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2022/11/11        namhyeop       최초 생성
 */
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
public class AnnualSaleDto {
    private Long january;
    private Long february;
    private Long march;
    private Long april;
    private Long may;
    private Long june;
    private Long july;
    private Long august;
    private Long september;
    private Long october;
    private Long november;
    private Long december;

    public static AnnualSaleDto createAnnualSaleDto(List<Long> annualeSaleInfo){
        return new AnnualSaleDto(annualeSaleInfo.get(1)
                ,annualeSaleInfo.get(2)
                ,annualeSaleInfo.get(3)
                ,annualeSaleInfo.get(4)
                ,annualeSaleInfo.get(5)
                ,annualeSaleInfo.get(6)
                ,annualeSaleInfo.get(7)
                ,annualeSaleInfo.get(8)
                ,annualeSaleInfo.get(9)
                ,annualeSaleInfo.get(10)
                ,annualeSaleInfo.get(11)
                ,annualeSaleInfo.get(12));
    }
}