
import { WebtoonSummaryDto } from '../../types/webtoon/WebtoonSummaryDto';

export function logUserActivity(userId: number, webtoon: WebtoonSummaryDto) {

    fetch('http://localhost:8080/activity/logs', {  
    method: 'POST',
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      userId: userId,
      webtoonId: webtoon.webtoonId,
      webtoonName: webtoon.webtoonName
    }),
  }).catch((error) => console.error('로그 저장 실패:', error));
}
