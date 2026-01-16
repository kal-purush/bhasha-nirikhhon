package com.boot.Controller;

import java.text.SimpleDateFormat;
import java.util.Date;import com.mysql.cj.log.Log;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DateUtils {

	public static Long calculateDaysDifference(String releaseDate) {
        if (releaseDate != null) {
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
                Date currentDate = dateFormat.parse(dateFormat.format(new Date()));
                Date releaseDate2 = dateFormat.parse(releaseDate);                
                log.info("@#@#currentDate===>"+currentDate);
                log.info("@#@#releaseDate2===>"+releaseDate2);

                // 날짜 차이 계산
                if(releaseDate2.getTime()>currentDate.getTime()) 
                {
                	long diffInMillies = Math.abs(releaseDate2.getTime() - currentDate.getTime());
                	log.info("@#@#diffInMillies===>"+diffInMillies);
                    return diffInMillies / 86400000L; // 밀리초에서 일 단위로 변환
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
	
}