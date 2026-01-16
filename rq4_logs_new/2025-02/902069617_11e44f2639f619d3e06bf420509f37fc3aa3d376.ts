import { useContext, useEffect, useMemo, useState } from "react";
import { Context } from "../App";
import { axiosInstance } from "../api/apiClient";
import { AMOUNT, GET_REASONS, ORDER_LIST } from "../api/Constants";

const useHomeData = () => {
  const {
    userInfo,
    setReasons,
    setRecieptTasks,
    setSendingTasks,
    recieptTasks,
    sendingTasks,
    amount,
    setAmount,
  } = useContext(Context);

  const [loading, setLoading] = useState(false);

  // Fetch reasons
  const fetchReasons = async () => {
    try {
      const response = await axiosInstance.get(GET_REASONS);
      setReasons(response.data.response);
    } catch (error) {
      console.error("Error fetching reasons:", error);
    }
  };

  // Fetch amount
  const fetchAmount = async () => {
    try {
      const response = await axiosInstance.get(AMOUNT, {
        params: {
          cashregistry_data: btoa(
            JSON.stringify({
              device_id: userInfo.device_id,
            })
          ),
        },
      });
      setAmount(response.data.response);
    } catch (error) {
      console.error("Error fetching amount:", error);
    }
  };

  // Fetch receipt tasks
  const fetchRecieptTasks = async () => {
    try {
      const response = await axiosInstance.get(ORDER_LIST, {
        params: {
          tasklist_data: btoa(
            JSON.stringify({
              device_id: userInfo.device_id,
              pickup_task: true,
              status: ["Waiting", "Accepted", "Completed", "Canceled"],
            })
          ),
        },
      });
      setRecieptTasks(response.data.response);
    } catch (error) {
      console.error("Error fetching receipt tasks:", error);
    }
  };

  // Fetch sending tasks
  const fetchSendingTasks = async () => {
    try {
      const response = await axiosInstance.get(ORDER_LIST, {
        params: {
          tasklist_data: btoa(
            JSON.stringify({
              device_id: userInfo.device_id,
              pickup_task: false,
              status: ["Waiting", "Accepted", "Completed", "Canceled"],
            })
          ),
        },
      });
      setSendingTasks(response.data.response);
    } catch (error) {
      console.error("Error fetching sending tasks:", error);
    }
  };

  useEffect(() => {
    const fetchAllData = async () => {
      setLoading(true);
      try {
        await fetchRecieptTasks();
        await fetchSendingTasks();
        await fetchAmount();
        await fetchReasons();
      } catch (error) {
        console.log(error);
      } finally {
        setLoading(false);
      }
    };

    fetchAllData();
  }, []);

  // Calculate task amounts using useMemo
  const taskAmounts = useMemo(() => {
    const newTaskAmounts = {
      receiptAccepted: 0,
      receiptCompleted: 0,
      receiptCanceled: 0,
      receiptWaiting: 0,
      sendingAccepted: 0,
      sendingCompleted: 0,
      sendingCanceled: 0,
      sendingWaiting: 0,
    };

    recieptTasks.forEach((task) => {
      if (task.Status === "Accepted") newTaskAmounts.receiptAccepted++;
      else if (task.Status === "Completed") newTaskAmounts.receiptCompleted++;
      else if (task.Status === "Canceled") newTaskAmounts.receiptCanceled++;
      else newTaskAmounts.receiptWaiting++;
    });

    sendingTasks.forEach((task) => {
      if (task.Status === "Accepted") newTaskAmounts.sendingAccepted++;
      else if (task.Status === "Completed") newTaskAmounts.sendingCompleted++;
      else if (task.Status === "Canceled") newTaskAmounts.sendingCanceled++;
      else newTaskAmounts.sendingWaiting++;
    });

    return newTaskAmounts;
  }, [recieptTasks, sendingTasks]);

  return { loading, amount, taskAmounts };
};

export default useHomeData;