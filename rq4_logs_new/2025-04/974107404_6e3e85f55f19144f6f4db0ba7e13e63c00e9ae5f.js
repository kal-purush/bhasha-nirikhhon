import { ref } from 'vue';
import api from '@/api';
import { useToast } from 'vue-toastification';

export function useLoginCustomer() {
  const email = ref('');
  const password = ref('');
  const loading = ref(false);
  const message = ref('');
  const toast = useToast();

  const login = async () => {
    loading.value = true;
    try {
      const response = await api.post('/customer/login', {
        email: email.value,
        password: password.value,
      });

      const token = response.data.token;
      localStorage.setItem('customer_token', token);

      toast.success('Logged in successfully!');
    } catch (error) {
      const errorMessage = error.response?.data?.message || 'Login failed.';
      message.value = errorMessage;
      toast.error(errorMessage);
    } finally {
      loading.value = false;
    }
  };

  return {
    email,
    password,
    loading,
    message,
    login,
  };
}