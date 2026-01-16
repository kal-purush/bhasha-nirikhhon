import {message} from "antd";
import {useEffect, useState} from "react";

interface UseNotificationProps {
  message: string | null;
}

export const useNotification = (initialState: UseNotificationProps | null) => {
  const [messageApi, contextHolder] = message.useMessage();
  const [messageData, setMessageData] = useState(initialState);

  useEffect(() => {
    if (messageData?.message) {
      messageApi.open({
        type: "error",
        content: messageData.message,
      });
    }
  }, [messageData]);

  return {contextHolder, setMessageData};
};