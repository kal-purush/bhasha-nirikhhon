import { Bounce, toast, ToastOptions } from "react-toastify";

const defaultToastOptions: ToastOptions = {
  position: "top-right",
  autoClose: 3000,
  hideProgressBar: false,
  closeOnClick: true,
  pauseOnHover: true,
  draggable: true,
  progress: undefined,
  theme: "light",
  transition: Bounce,
};

const defaultErrorText = "Error! Please refresh the page!";
const defaultSuccessText = "Success!";

export function showToastError(text = defaultErrorText) {
  toast.error(text, defaultToastOptions);
}
export function showToastSuccess(text = defaultSuccessText) {
  toast.success(text, defaultToastOptions);
}