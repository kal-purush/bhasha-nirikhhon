package ywphsm.ourneighbor.domain.store;

import ywphsm.ourneighbor.domain.embedded.BusinessTime;
import ywphsm.ourneighbor.repository.store.dto.SimpleSearchStoreDTO;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.TextStyle;
import java.util.Locale;

public class StoreUtil {

    /*
        Status 업데이트
         => 검색시 DTO에만 반영되게 만들고 DB에는 반영되지 않음
         => DB 자체 값은 항상 OPEN
     */
    public static void autoUpdateStatus(SimpleSearchStoreDTO dto) {
        String today = LocalDate.now().getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.KOREAN);             // 오늘 요일 한글로 변경

        LocalTime time = LocalTime.now();

        BusinessTime businessTime = dto.getBusinessTime();

        if (!dto.getOffDays().isEmpty()) {
            for (String offDay : dto.getOffDays()) {
                if (today.equals(offDay)) {
                    dto.setStatus(StoreStatus.CLOSED);
                    return;
                }
            }
        }

        /*
            null인 경우를 처리하지 않으면 에러 발생
              => 검색 결과가 2개 이상인 경우 그냥 터져버림
         */
        if (businessTime.getOpeningTime() == null || businessTime.getClosingTime() == null) {
            return;
        }

        if (businessTime.getOpeningTime().equals(businessTime.getClosingTime())) {
            dto.setStatus(StoreStatus.OPEN);
            return;
        }

        if (!(time.isAfter(businessTime.getOpeningTime()) && time.isBefore(businessTime.getClosingTime()))) {
            dto.setStatus(StoreStatus.CLOSED);
            return;
        }

        if (businessTime.getBreakStart() == null) {
            return;
        }

        if (time.isAfter(businessTime.getBreakStart()) && time.isBefore(businessTime.getBreakEnd())) {
            dto.setStatus(StoreStatus.BREAK);
        }
    }
}