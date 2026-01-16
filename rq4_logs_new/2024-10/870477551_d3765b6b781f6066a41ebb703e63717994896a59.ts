import {create} from 'zustand';

 
const useToastStore = create((set) => ({
  message: '',
  visible: false,
  
 
  showToast: (message) => {
    set({ message, visible: true });
    setTimeout(() => set({ visible: false }), 3000);  
  },


}));

export default useToastStore;