import { AxiosError } from "axios";

export function axiosErrorHandle(
  error: unknown,
  setState: (value: any) => void
) {
  const axiosError = error as AxiosError;
  if (axiosError.response) {
    console.log((axiosError.response.data as { error: string }).error);
    setState(
      `エラーが発生しました。画面を更新してください。 ${
        (axiosError.response.data as { error: string }).error
      }`
    );
  } else {
    console.log("サーバーエラーが発生しました。");
    setState("サーバーエラーが発生しました。");
  }
}