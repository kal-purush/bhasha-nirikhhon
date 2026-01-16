// hooks/useAlertModal.ts
import { useState } from "react";

type AlertType = "error" | "success" | "warning" | "info";

interface AlertState {
  isOpen: boolean;
  message: string;
  type: AlertType;
  showConfirmButton: boolean;
  confirmText: string;
}

export default function useAlertModal() {
  const [alertState, setAlertState] = useState<AlertState>({
    isOpen: false,
    message: "",
    type: "info",
    showConfirmButton: true,
    confirmText: "확인",
  });

  const showAlert = (
    message: string,
    type: AlertType = "info",
    showConfirmButton: boolean = true,
    confirmText: string = "확인"
  ) => {
    setAlertState({
      isOpen: true,
      message,
      type,
      showConfirmButton,
      confirmText,
    });
  };

  const closeAlert = () => {
    setAlertState((prev) => ({ ...prev, isOpen: false }));
  };

  return {
    alertState,
    showAlert,
    closeAlert,
  };
}