import axios from 'axios';

export const deleteDrug = async (
  drugName: string,
  intakeStart: string,
  intakeEnd: string,
): Promise<void> => {
  const token = localStorage.getItem('token');

  if (!token) {
    throw new Error('토큰 없음');
  }

  await axios.delete(`http://localhost:8080/api/v1/medicines/records`, {
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      Authorization: `Bearer ${token}`,
    },
    withCredentials: true,

    data: {
      drugName,
      intakeStart,
      intakeEnd,
    },
  });
};