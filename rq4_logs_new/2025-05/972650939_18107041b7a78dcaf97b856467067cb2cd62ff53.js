import { defineStore } from 'pinia';
import axios from 'axios';

export const useAuthStore = defineStore('auth', {
    state: () => ({
        token: localStorage.getItem('token') || null,
        user: null,
    }),

    actions: {
        // Login
        async login(credentials) {
            const response = await axios.post(`${import.meta.env.VITE_API_URL}/login`, credentials);
            localStorage.setItem('token', this.token);

            axios.defaults.headers.common['Authorization'] = `Bearer ${this.token}`;
        },

        // Get auth user info
        async getUser() {
            const response = await axios.get(`${import.meta.env.VITE_API_URL}/me`);
            this.user = response.data.data;
        },

        // Logout user
        logout() {
            this.token = null;
            this.user = null;
            localStorage.removeItem('token');
            delete axios.defaults.headers.common['Authorization'];
        },
    },
})