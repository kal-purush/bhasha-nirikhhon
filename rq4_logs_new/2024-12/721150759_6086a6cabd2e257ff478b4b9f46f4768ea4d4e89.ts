import Swal from "sweetalert2";
import "src/views/auth/styles.css";

type SweetAlertOptions = {
  title: string;
  message: string;
  icon?: "success" | "error" | "warning" | "info" | "question";
  confirmText?: string;
  onConfirm?: () => void;
};

export const useSweetAlert = () => {
  const showAlert = ({
    title,
    message,
    icon = "success",
    confirmText = "Aceptar",
    onConfirm,
  }: SweetAlertOptions) => {
    Swal.fire({
      title,
      text: message,
      icon,
      confirmButtonText: confirmText,
      customClass: {
        title: "swal-title",
        confirmButton: "swal-button",
      },
    }).then((result) => {
      if (result.isConfirmed && onConfirm) {
        onConfirm();
      }
    });
  };
  return { showAlert };
};