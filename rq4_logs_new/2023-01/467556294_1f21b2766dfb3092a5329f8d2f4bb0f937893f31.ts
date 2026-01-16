import { StyleSheet } from 'react-native';

import { materialColors } from '@/ThemeProvider';

export const styles = StyleSheet.create({
	image: {
		paddingTop: '56.25%',
		backgroundColor: materialColors.grey[300],
		height: 150,
	},
	memoTextInput: {
		backgroundColor: '#fff',
	},
	chipContainer: { flexDirection: 'row' },
	tagsChip: {
		backgroundColor: materialColors.grey['50'],
	},
	tagsChipText: {
		color: materialColors.grey[800],
	},
	spanLabel: { color: materialColors.grey[700] },
	spanTextInput: {
		backgroundColor: '#fff',
	},
	representativeStartDateTimeLabel: { color: materialColors.grey[700] },
	representativeStartDateTimePicker: { backgroundColor: '#fff' },
	captionTextInput: {
		height: 400,
		backgroundColor: '#fff',
	},
});