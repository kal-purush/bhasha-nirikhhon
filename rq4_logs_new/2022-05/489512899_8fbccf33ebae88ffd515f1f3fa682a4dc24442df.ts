import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react'
import { IUser } from "../../interface/userInteface";

interface Itoken {
    token: string
}

export const authApi = createApi({
    reducerPath: 'authApi',
    baseQuery: fetchBaseQuery({baseUrl: `http://localhost:5000/api/user`}),
    endpoints: (build) => ({
        getLogin: build.mutation<Itoken, IUser>({
            query: (body) => ({
                url: '/login',
                method: "POST",
                body
            })
        }),
        getRegister: build.mutation<Itoken, IUser>({
            query: (body) =>({
                url: '/registration',
                method: "POST",
                body
            })
        })
    })
})


export const {useGetLoginMutation} = authApi