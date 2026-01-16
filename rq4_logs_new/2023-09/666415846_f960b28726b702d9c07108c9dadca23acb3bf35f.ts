import { PayloadAction, createSlice } from '@reduxjs/toolkit';
import { fetchAllTopics } from './forumThunk';

export const forumSlice = createSlice({
	name: 'forum',
	initialState: {},
	reducers: {},
	extraReducers: builder => {
		builder
			.addCase(
				fetchAllTopics.fulfilled,
				(state, action) => {
					//state.data = action.payload;
					//state.isDataLoaded = true;
				},
			)
			.addCase(fetchAllTopics.rejected, () => {
				console.error('fetch topics failed');
			});
	},
});

export default forumSlice.reducer;