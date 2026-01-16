'use server';

import { ParkingInfo } from '@/types/parkingInfo';

const parsePrivateParkingInfo = (
	parkingInfo: any,
	parkingInfoDetail: any,
	realtimeParkingInfo: any
): ParkingInfo[] => {
	return parkingInfo.map((v: any) => {
		const detail = parkingInfoDetail.find((info: any) => info.prk_center_id === v.prk_center_id);
		const realTimeInfo = realtimeParkingInfo.find(
			(info: any) => info.prk_center_id === v.prk_center_id
		);

		return {
			name: v.prk_plce_nm,
			addr: v.prk_plce_adres,
			telNo: '',
			totalParkingCount: v.prk_cmprt_co,
			latitude: v.prk_plce_entrc_la,
			longitude: v.prk_plce_entrc_lo,
			baseParkingFee: parseInt(detail.basic_info.parking_chrge_bs_chrg),
			baseParkingTime: parseInt(detail.basic_info.parking_chrge_bs_time),
			additionalParkingFee: parseInt(detail.basic_info.parking_chrge_adit_unit_chrge),
			additionalParkingTime: parseInt(detail.basic_info.parking_chrge_adit_unit_time),
			monthlyParkingFee: parseInt(detail.fxamt_info.parking_chrge_mon_unit_chrge),
			weekdayOperatingHours: `${detail.Monday.opertn_start_time.slice(0, 2)}:${detail.Monday.opertn_start_time.slice(2, 4)} ~ ${detail.Monday.opertn_end_time.slice(0, 2)}:${detail.Monday.opertn_end_time.slice(2, 4)}`,
			weekendOperatingHours: `${detail.Saturday.opertn_start_time.slice(0, 2)}:${detail.Saturday.opertn_start_time.slice(2, 4)} ~ ${detail.Saturday.opertn_end_time.slice(0, 2)}:${detail.Saturday.opertn_end_time.slice(2, 4)}`,
			holidayOperatingHours: `${detail.Holiday.opertn_start_time.slice(0, 2)}:${detail.Holiday.opertn_start_time.slice(2, 4)} ~ ${detail.Holiday.opertn_end_time.slice(0, 2)}:${detail.Holiday.opertn_end_time.slice(2, 4)}`,
			availableParkingSpots: realTimeInfo?.pkfc_Available_ParkingLots_total,
		};
	});
};

export const getPrivateParkingInfo = async (): Promise<ParkingInfo[]> => {
	const response = {
		resultCode: '0',
		resultMsg: 'SUCCESS',
		numOfRows: '10',
		pageNo: '1',
		totalCount: '100',
		PrkSttusInfo: [
			{
				prk_center_id: '12345-67890-12345-12-1',
				prk_plce_nm: '서울시 망원동 주차장1',
				prk_plce_adres: '서울시 망원동 월드컵로 1길',
				prk_plce_entrc_la: '35.879337',
				prk_plce_entrc_lo: '128.628764',
				prk_cmprt_co: '100',
			},
			{
				prk_center_id: '12345-67890-12345-12-2',
				prk_plce_nm: '서울시 망원동 주차장2',
				prk_plce_adres: '서울시 망원동 월드컵로 2길',
				prk_plce_entrc_la: '35.879337',
				prk_plce_entrc_lo: '128.628764',
				prk_cmprt_co: '200',
			},
		],
	};

	const parkingInfoDetail = getPrivateParingInfoDetail();
	const realtimeParkingInfo = getRealtimeParkingInfo();

	return parsePrivateParkingInfo(response.PrkSttusInfo, parkingInfoDetail, realtimeParkingInfo);
};

const getPrivateParingInfoDetail = () => {
	const response = {
		resultCode: '0',
		resultMsg: 'SUCCESS',
		numOfRows: '10',
		pageNo: '1',
		totalCount: '100',
		PrkOprInfo: [
			{
				prk_center_id: '12345-67890-12345-12-1',
				Sunday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Monday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Tuesday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Wednesday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Thursday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Friday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Saturday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Holiday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				opertn_bs_free_time: '30',
				basic_info: {
					parking_chrge_bs_time: '10',
					parking_chrge_bs_chrge: '1000',
					parking_chrge_adit_unit_time: '20',
					parking_chrge_adit_unit_chrge: '1000',
				},
				fxamt_info: {
					parking_chrge_one_day_chrge: '10000',
					parking_chrge_mon_unit_chrge: '50000',
				},
			},
			{
				prk_center_id: '12345-67890-12345-12-2',
				Sunday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Monday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Tuesday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Wednesday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Thursday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Friday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Saturday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				Holiday: {
					opertn_start_time: '080000',
					opertn_end_time: '200000',
				},
				opertn_bs_free_time: '30',
				basic_info: {
					parking_chrge_bs_time: '10',
					parking_chrge_bs_chrge: '1000',
					parking_chrge_adit_unit_time: '20',
					parking_chrge_adit_unit_chrge: '1000',
				},
				fxamt_info: {
					parking_chrge_one_day_chrge: '10000',
					parking_chrge_mon_unit_chrge: '50000',
				},
			},
		],
	};

	return response.PrkOprInfo;
};

const getRealtimeParkingInfo = () => {
	const response = {
		resultCode: '0',
		resultMsg: 'SUCCESS',
		numOfRows: '10',
		pageNo: '1',
		totalCount: '100',
		PrkRealtimeInfo: [
			{
				prk_center_id: '12345-67890-12345-12-1',
				pkfc_ParkingLots_total: '10',
				pkfc_Available_ParkingLots_total: '5',
			},
			{
				prk_center_id: '12345-67890-12345-12-2',
				'pkfc-ParkingLots-total': '10',
				pkfc_Available_ParkingLots_total: '5',
			},
		],
	};

	return response.PrkRealtimeInfo;
};