import axios from 'axios'

const axiosIns = axios.create({
    baseURL: "https://node-crud-api-0n4d.onrender.com"
})


const UserApi = {
    readAll: async () => {
        return axiosIns.request({
            url: `/api/user`,
            method: "GET"
        })
    },
    readSingle: async (id) => {
        return axiosIns.request({
            url: `/api/user/single/${id}`,
            method: "GET"
        })
    },
    createUser: async (user) => {
        return axiosIns.request({
            url: `/api/user/add`,
            method: "POST",
            data: user
        })
    },
    updateUser: async (user,id) => {
        return axiosIns.request({
            url: `/api/user/update/${id}`,
            method: "PATCH",
            data: user
        })
    },
    deleteUser: async (id) => {
        return axiosIns.request({
            url: `/api/user/delete/${id}`,
            method: "DELETE"
        })
    }
}

export default UserApi