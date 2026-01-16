import { parkInfoContext } from '@/providers/ParkInfoProvider';
import { Location } from '@/types/map';
import { Radius } from '@/types/radius';
import { calculateHaversineDistance } from '@/utils/calculateHaversinceDistance';
import { useCallback, useContext } from 'react';

const useParkInfo = () => {
	const { parkInfos, parkingInfos } = useContext(parkInfoContext);

	const getTargetParkInfos = useCallback(
		(location: Location, radius: Radius) =>
			parkInfos?.filter((parkInfo) => {
				const distance = calculateHaversineDistance({
					lat1: location.latitude,
					lng1: location.longitude,
					lat2: parkInfo.LAT,
					lng2: parkInfo.LOT,
				});
				return distance <= radius;
			}) ?? [],
		[parkInfos]
	);

	return {
		parkInfos,
		parkingInfos,
		getTargetParkInfos,
	};
};

export default useParkInfo;