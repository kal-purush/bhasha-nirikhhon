import { axiosClient } from '../../../apis/client';

const handleWhatsAppClick = async (phone: string) => {
  try {
    let processedPhone = phone.replace(/-/g, '');
    if (processedPhone.startsWith('0')) {
      processedPhone = '972' + processedPhone.slice(1);
    }
    const response = await axiosClient.get(`/api/v1/external/whatsapp/${processedPhone}`);
    const data = response.data;

    window.open( data.url, '_blank');
  } catch (error) {
    console.error("Error opening WhatsApp chat:", error);
  }
};

export {handleWhatsAppClick}