import { useState } from 'react';
import { useFieldArray, useForm, Controller } from 'react-hook-form';
import { toast } from 'react-toastify';
import fetchWithToken from 'API/api';

const daysOfWeek = [
  { label: 'Monday', value: '1' },
  { label: 'Tuesday', value: '2' },
  { label: 'Wednesday', value: '3' },
  { label: 'Thursday', value: '4' },
  { label: 'Friday', value: '5' },
  { label: 'Saturday', value: '6' },
  { label: 'Sunday', value: '7' },
];

const MAX_NOTIFICATIONS = 5;

const useLearning = () => {
  const [isAddButtonDisabled, setIsAddButtonDisabled] = useState(false);

  const { control, handleSubmit, setValue, watch } = useForm({
    defaultValues: {
      notifications: [{ time: '', type: '1' }],
      reviewDays: '6',
      offDays: '7',
    },
  });

  const { fields, append, remove } = useFieldArray({
    control,
    name: 'notifications',
  });

  const formatTime = (time: Date) => {
    const hours = time.getHours().toString().padStart(2, '0');
    const minutes = time.getMinutes().toString().padStart(2, '0');
    return `${hours}:${minutes}`;
  };

  const onSubmit = async (data: any) => {
    const formattedData = {
      ...data,
      notifications: data.notifications.map((notification: any) => ({
        time: notification.time ? formatTime(new Date(notification.time)) : '',
        type: notification.type || '1',
      })),
    };
    console.log(formattedData);

    try {
      const response = await fetchWithToken({
        endpoint: 'updateUserSettings',
        method: 'PUT',
        body: formattedData,
      });
      console.log('Response:', response);
      toast.success('Settings updated successfully');
    } catch (error) {
      console.error('Error updating settings:', error);
      toast.error('Failed to update settings');
    }
  };

  const handleAddNotification = () => {
    const numberOfNotifications = fields.length;
    if (numberOfNotifications >= MAX_NOTIFICATIONS) {
      toast.warning(`You have reached the maximum number of notifications ${MAX_NOTIFICATIONS}`);
      setIsAddButtonDisabled(true);
    } else {
      append({ time: '', type: '1' });
    }
  };

  const handleRemoveNotification = (index: number) => {
    remove(index);
    if (fields.length <= MAX_NOTIFICATIONS) {
      setIsAddButtonDisabled(false);
    }
  };

  return {
    handleRemoveNotification,
    handleAddNotification,
    isAddButtonDisabled,
    daysOfWeek,
    watch,
    handleSubmit,
    setValue,
    onSubmit,
    fields,
    control,
  };
};

export default useLearning;