import { useNavigate } from "react-router-dom";
import { useContext } from "react";
import { Context } from "../App";
import { t } from "i18next";

const useOpenActiveTask = () => {
  const { sendingTasks } = useContext(Context);
  const navigate = useNavigate();

  const handleConfirmAllTasks = (selectedOrders: { [key: string]: boolean }) => {
    const selectedOrdersList = Object.keys(selectedOrders)
      .filter((trackingCode) => selectedOrders[trackingCode])
      .map((trackingCode) => {
        console.log("trackingCode", trackingCode);
        const order = sendingTasks.find(
          (task) => task.tracking_code === trackingCode
        );

        if (!order) {
          console.warn(`Order with tracking code ${trackingCode} not found`);
          return null; // Return null if order is not found
        }

        return {
          tracking_code: order.tracking_code,
          sum: order.sum,
          client_address: order.client_address,
          client_phone: order.client_phone,
        };
      })
      .filter((order) => order !== null);

    if (selectedOrdersList.length === 0) {
      alert(t("No orders selected!"));
      return;
    }

    // Group orders by client_phone
    const phoneGroups: { [key: string]: any[] } = {};
    selectedOrdersList.forEach((order) => {
      if (!phoneGroups[order.client_phone]) {
        phoneGroups[order.client_phone] = [];
      }
      phoneGroups[order.client_phone].push(order);
    });

    // Get the first group (first phone number)
    const firstPhoneNumber = Object.keys(phoneGroups)[0];
    const firstGroupOrders = phoneGroups[firstPhoneNumber];

    const firstAddress = selectedOrdersList[0].client_address;

    const differentAddressOrders = selectedOrdersList.filter(
      (order) => order.client_address != firstAddress
    );

    console.log("First group orders:", firstGroupOrders);

    // Open the first order and pass others as additional codes
    navigate(`/order/${firstGroupOrders[0].tracking_code}`, {
      state: { selectedOrdersList, additionalOrders: firstGroupOrders.slice(1) , differentAddressOrders},
    });
  };

  return { handleConfirmAllTasks };
};

export default useOpenActiveTask;