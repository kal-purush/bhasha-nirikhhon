package study.memoravel.controller.dto;


import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import study.memoravel.dto.RegisteredCourseCard;

import java.util.Date;
import java.util.List;

@Getter
@ApiModel("등록된 코스 정보 카드")
@AllArgsConstructor
public class RegisteredCourseCardResponse {
    @ApiModelProperty("코스 아이디")
    private final long id;

    @ApiModelProperty("코스 이름")
    private final String title;

    @ApiModelProperty("대표 지역 주소")
    private final String representRegion;

    @ApiModelProperty("가능한 언어 리스트")
    private final List<String> enableLangList;

    @ApiModelProperty("사진 주소 리스트")
    private final List<String> imagePathList;

    @ApiModelProperty("코스 설명 글")
    private final String contents;

    @ApiModelProperty("코스 가이드 비용")
    private final int guideCost;

    @ApiModelProperty("코스 총 예상 비용")
    private final int expectedTotalCost;

    @ApiModelProperty("시작 날짜(yyyy-MM-dd'T'HH:mm:ss.SSSXXX)")
    private final Date startDate;

    @ApiModelProperty("종료 날짜(yyyy-MM-dd'T'HH:mm:ss.SSSXXX)")
    private final Date endDate;

    public RegisteredCourseCardResponse(RegisteredCourseCard registeredCourseCard) {
        id = registeredCourseCard.getId();
        title = registeredCourseCard.getTitle();
        representRegion = registeredCourseCard.getRepresentRegion();
        enableLangList = registeredCourseCard.getEnableLangList();
        imagePathList = registeredCourseCard.getImagePathList();
        contents = registeredCourseCard.getContents();
        guideCost = registeredCourseCard.getGuideCost();
        expectedTotalCost = registeredCourseCard.getExpectedTotalCost();
        startDate = registeredCourseCard.getStartDate();
        endDate = registeredCourseCard.getEndDate();
    }
}