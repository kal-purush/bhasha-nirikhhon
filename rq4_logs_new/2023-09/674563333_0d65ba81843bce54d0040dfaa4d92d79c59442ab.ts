import axios from "..";
import { useQuery } from "@tanstack/react-query";
import { HumTempReading, Reading } from "./types";

const playSound = (url: string) => {
  const audio = new Audio(url);
  audio.play();
};

const beep =
  "https://res.cloudinary.com/matec-technology-services/video/upload/v1692170386/smartflock/mixkit-appliance-ready-beep-1076_dzhdck.wav";

const checkAndNotify = (value: number) => {
  if (value > 8) {
    const noti = new Notification(`SMARTLOCK`, {
      body: `New notification`,
      icon: "SMARTLOCK"
    });

    noti.onclick = () => {
      window.open(`/dashboard/overview`);
      noti.close();
    };

    noti.onshow = () => {
      beep && playSound(beep);
    };
  }
};

/**
 * org get all gas reading
 * @returns
 */
const _getGasReadingsRequest = async (): Promise<
  ApiResponse<{ message: string; data: Reading[] }>
> => {
  const { data } = await axios.get("gas_reading");
  return data;
};

/**
 * hook wrapper
 * @param user
 * @returns
 */
export const useGetGasReadingsQuery = () =>
  useQuery({
    queryKey: ["gas_reading"],
    queryFn: () => _getGasReadingsRequest(),
    refetchInterval: 60000,
    onSuccess: (data) => {
      const latestAmmoniaValue = data?.payload?.data?.slice(-1)[0]?.value || 0;
      // checkAndNotify(latestAmmoniaValue);
    }
  });

/**
 * org get all water level reading
 * @returns
 */
const _getWaterLevelRequest = async (): Promise<
  ApiResponse<{ message: string; data: Reading[] }>
> => {
  const { data } = await axios.get("water_level");
  return data;
};

/**
 * hook wrapper
 * @param user
 * @returns
 */
export const useGetWaterLevelQuery = () =>
  useQuery({
    queryKey: ["water_level"],
    queryFn: () => _getWaterLevelRequest(),
    refetchInterval: 60000
  });

/**
 * org get all gas reading
 * @returns
 */
const _getHumTempReadingRequest = async (): Promise<
  ApiResponse<{ message: string; data: HumTempReading[] }>
> => {
  const { data } = await axios.get("water_level");
  return data;
};

/**
 * hook wrapper
 * @param user
 * @returns
 */
export const useGetHumTempReadingQuery = () =>
  useQuery({
    queryKey: ["water_level"],
    queryFn: () => _getHumTempReadingRequest(),
    refetchInterval: 60000
  });