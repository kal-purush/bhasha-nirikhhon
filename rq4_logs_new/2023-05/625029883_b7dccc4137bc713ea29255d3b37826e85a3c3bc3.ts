import dayjs from 'dayjs';

export const rules = {
    isDateValid: (selectedDate: dayjs.Dayjs | null) => {
      if (!selectedDate) {
        // Если дата не выбрана, то валидация не пройдена
        return false;
      }
      
      // Проверяем, что выбранная дата не является прошедшей
      const today = dayjs();
      return selectedDate.isAfter(today, 'day');
    }
}