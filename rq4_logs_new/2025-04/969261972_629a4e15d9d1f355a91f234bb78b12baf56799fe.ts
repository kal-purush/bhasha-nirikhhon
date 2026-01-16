export const validateOtp = async (otp: string, ref: string) => {
  try {
    const response = await fetch("http://localhost:3000/auth/validate-otp", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ code: otp, ref }),
    });

    if (!response.ok) {
      throw new Error("Invalid OTP");
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error validating OTP:", error);
    throw error;
  }
};