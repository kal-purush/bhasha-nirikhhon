import { baseApi } from './baseApi'
import {
    Timeline,
} from '@/types/timeline'

export const timelineApi = baseApi.injectEndpoints({
    endpoints: (builder) => ({
        getTimelinesByEvent: builder.query<Timeline[], { event_id: string }>({
            query: ({event_id}) => ({
                url: `/timelines/${event_id}/`,
                method: 'GET',
            }),
            providesTags: ['Timeline'],
        }),
    }),
})

export const {
    useGetTimelinesByEventQuery,
} = timelineApi