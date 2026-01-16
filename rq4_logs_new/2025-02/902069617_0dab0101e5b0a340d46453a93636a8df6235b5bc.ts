import { useState, useContext } from "react";

declare global {
  interface Window {
    recaptchaVerifier: any;
  }
}
import { useNavigate } from "react-router-dom";
import { RecaptchaVerifier, signInWithPhoneNumber, ConfirmationResult } from "firebase/auth";
import { auth } from "../config/firebase";
import { axiosInstance } from "../api/apiClient";
import { BOT_AUTH, CHECK_OTP, SEND_OTP } from "../api/Constants";
import { Context } from "../App";


export const useAuth = () => {
  const navigate = useNavigate();
  const { setUserInfo, userInfo } = useContext(Context);
  const [loading, setLoading] = useState(false);
  const [errorKey, setErrorKey] = useState("");
  const [showRegister, setShowRegister] = useState(false);
  const [showOtpField, setShowOtpField] = useState(false);
  const [confirmationResult, setConfirmationResult] = useState<ConfirmationResult | null>(null);

  const handleSignIn = async () => {
    setLoading(true);
    try {
      const response = await axiosInstance.get(BOT_AUTH, {
        params: { telegram_id: userInfo.telegram_id  },
      });

      setUserInfo((prev: any) => ({
        ...prev,
        name: response.data.response.courier_name,
        device_id: response.data.response.device_id,
      }));

      if (response.status === 200) {
        navigate("/home");
      } else {
        setShowRegister(true);
      }
    } catch (error: any) {
      setShowRegister(true);
      setErrorKey("no_account_register");
    } finally {
      setLoading(false);
    }
  };

  const handleRegister = async (phoneNumber: string) => {
    if (!phoneNumber.trim()) {
      setErrorKey("phone_number_required");
      return;
    }
    setLoading(true);

    try {
        const telegramResponse =  await axiosInstance.post(SEND_OTP, {
        telegram_id: userInfo.telegram_id,
        phone_number: phoneNumber,
        type: "1",
      });

      if (telegramResponse.data.status === "ok") {
        
        setErrorKey("");

        if (!window.recaptchaVerifier) {
            window.recaptchaVerifier = new RecaptchaVerifier(auth, "recaptcha-container", {
              size: "normal",
              callback: () => {
                console.log("reCAPTCHA verified!");
              },
              "expired-callback": () => {
                setErrorKey("reCAPTCHA expired. Please refresh and try again.");
              },
            });
  
            await window.recaptchaVerifier.render();
        }
            const appVerifier = window.recaptchaVerifier;
            const result = await signInWithPhoneNumber(auth, phoneNumber, appVerifier);

            setConfirmationResult(result);
            setShowOtpField(true);
        } else {
            setErrorKey("error_sending_otp_telegram");
        }
    } catch (err: any) {
      setErrorKey("error_sending_otp");
    } finally {
      setLoading(false);
    }
  };

  const handleConfirmOtp = async (phoneNumber: string, otp: string) => {
    if (!otp.trim()) {
      setErrorKey("otp_required");
      return;
    }
    setLoading(true);

    try {
      const response = await axiosInstance.get(CHECK_OTP, {
        params: {
          phone_number: phoneNumber,
          otp,
          telegram_id: userInfo.telegram_id?.toString(),
        },
      });

      if (response.data.status === "ok") {
        if (!confirmationResult) {
          setErrorKey("no_firebase_otp_found");
          return;
        }

        await confirmationResult.confirm(otp);
        setUserInfo((prev: any) => ({
          ...prev,
          name: response.data.response.courier_name,
          device_id: response.data.response.device_id,
        }));

        navigate("/home");
      } else {
        setErrorKey("error_confirming_otp_telegram");
      }
    } catch (err) {
      setErrorKey("error_confirming_otp");
    } finally {
      setLoading(false);
    }
  };

  return {
    handleSignIn,
    handleRegister,
    handleConfirmOtp,
    loading,
    errorKey,
    showRegister,
    showOtpField,
  };
};