import { useEffect, useState } from "react";
import { ApplicationVerifier, RecaptchaVerifier } from "@firebase/auth";
import { auth } from "@/firebase";

const useRecaptcha = (containerId: string) => {
  const [recaptcha, setRecaptcha] = useState<ApplicationVerifier>();

  useEffect(() => {
    const recaptchaVerifier = new RecaptchaVerifier(auth, containerId, {
      size: "invisible",
    });

    setRecaptcha(recaptchaVerifier);

    return () => {
      recaptchaVerifier.clear();
    };
  }, [containerId]);

  return recaptcha;
};

export default useRecaptcha;