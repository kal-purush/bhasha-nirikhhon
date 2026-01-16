import { StyleSheet } from 'react-native';

import { materialColors } from '@/ThemeProvider';

export const styles = StyleSheet.create({
	itinerayImage: { width: '100%', paddingTop: '100%', backgroundColor: materialColors.grey[300] },
	itinerarySubTitle: {
		marginVertical: 16,
	},
	itineraryCaption: {
		marginVertical: 16,
	},
	planImage: {
		paddingTop: '56.25%',
		backgroundColor: materialColors.grey[300],
		height: 150,
	},
	planTitle: {
		position: 'absolute',
		top: 0,
		left: 0,
		color: 'white',
		backgroundColor: 'rgba(0,0,0,0.5)',
		paddingHorizontal: 8,
		paddingVertical: 4,
	},
	planTime: {
		position: 'absolute',
		bottom: 0,
		left: 0,
		color: 'white',
		backgroundColor: 'rgba(0,0,0,0.5)',
		paddingHorizontal: 8,
		paddingVertical: 4,
	},
	transportationSpanContainer: {
		flexDirection: 'row',
		flex: 12,
	},
	transportationSpanLeft: {
		flex: 2,
		height: 24,
	},
	transportationSpanRight: {
		flex: 10,
		flexDirection: 'row',
		alignItems: 'center',
		borderLeftWidth: 1,
		paddingHorizontal: 8,
	},
});