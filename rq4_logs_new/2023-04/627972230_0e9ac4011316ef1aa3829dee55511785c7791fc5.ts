import { toast } from "react-toastify";

export function capitalizeFirstLetter(string = "") {
  return string.charAt(0).toUpperCase() + string.slice(1);
}

export function notify(
  message: string,
  status: "warning" | "success" | "error" | "info",
  toastId?: string
) {
  if (status === "success")
    toast.success(capitalizeFirstLetter(message), {
      toastId: toastId || encodeURI(message),
    });
  if (status === "error")
    toast.error(capitalizeFirstLetter(message), {
      toastId: toastId || encodeURI(message),
    });
  if (status === "info")
    toast.info(capitalizeFirstLetter(message), {
      toastId: toastId || encodeURI(message),
    });
  if (status === "warning")
    toast.warning(capitalizeFirstLetter(message), {
      toastId: toastId || encodeURI(message),
    });
}

export function axiosErrorNotifier(message: string) {
  notify(
    message || "Poor connection, cannot fetch data",
    "error",
    "axios-error"
  );
}