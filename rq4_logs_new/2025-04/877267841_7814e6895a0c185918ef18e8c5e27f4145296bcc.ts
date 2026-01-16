import { locations } from "../../../utils/constants";

export const filterList = [
    {
        id: 1,
        title: 'Talent Badge',
        options: [
            { id: 'top_rated', title: 'Top Rated' },
            { id: 'verified', title: 'Verified' },
            { id: 'new', title: 'New' },
        ]
    },
    {
        id: 2,
        title: 'Category',
        options: [
            { id: 'mehanic', title: 'Mehanic' },
            { id: 'body_specialist', title: 'Electrician' },
            { id: 'exhaust', title: 'Exhaust' },
        ]
    },
    {
        id: 3,
        title: 'Location',
        options: locations.map((location) => ({ id: location, title: location }))
    },
    {
        id: 4,
        title: 'Availability',
        options: [
            { id: 'online', title: 'Online' },
            { id: 'offline', title: 'Offline' },
        ]
    },
]