package bit.anniversary.scheduled;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import bit.anniversary.sevice.AnService;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AnScheduled {

	private final AnService anService;

	@Scheduled(cron = "0 0 9 * * ?") // 매일 아침 9시에 실행
	@Transactional(readOnly = true)
	public void sendAnniversaryNotifications() {
		anService.sendAnniversaryNotifications();
	}

}