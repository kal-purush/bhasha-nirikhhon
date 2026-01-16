import axios from 'axios';

export const apiGetReviewTable = (reviewUid :number) =>
	axios({
		method: 'get',
		url: '/api/public/home/review/list',
    params: {
			business_shop_uid : reviewUid,
			video_type: 0,
			second_type: 0,
			is_filter: 0,
			offset: 0
		}
	})