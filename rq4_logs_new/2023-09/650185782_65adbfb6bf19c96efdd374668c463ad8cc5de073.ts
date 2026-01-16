// api.ts
import axios from "axios";

export const api = axios.create({
  baseURL: 'http://localhost:3000', // Coloque a URL correta do seu backend
});


export const useApi = () => ({

    validateToken: async (token: string, id: string) => {
        try {
          const response = await api.get(`api/user/?userId=${Number(id)}`, {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          });
      
          return response.data;
        } catch (error) {
          console.error('Erro ao validar token:', error);
          throw error;
        }
      },

    signIn: async (email: string, password: string) => {
        try {
          const response = await api.post("/api/auth/sign-in", {
            email,
            password,
          });

          return response  
        } catch (error) {
          console.error('Erro ao fazer login:', error);
          throw error;
        }
      }
      
      
})