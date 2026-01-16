/* eslint-disable unicorn/no-null */
import { create } from "zustand";

import { newsletterFormData } from "~/schemas";

const BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3000";
const API_URL = `${BASE_URL}/external/newsletter`;

interface NewsletterFormState {
  isSubmitting: boolean;
  responseMessage: string | null;
  setIsSubmitting: (isSubmitting: boolean) => void;
  setResponseMessage: (message: string | null) => void;
}

export const useNewsletterFormStore = create<NewsletterFormState>((set) => ({
  isSubmitting: false,
  responseMessage: null,
  setIsSubmitting: (isSubmitting: boolean) => set({ isSubmitting }),
  setResponseMessage: (message: string | null) =>
    set({ responseMessage: message }),
}));

// Hook for form submission logic
export const useSubmitNewsletterForm = () => {
  const { setIsSubmitting, setResponseMessage } = useNewsletterFormStore();

  return async (data: newsletterFormData) => {
    setIsSubmitting(true);

    try {
      const response = await fetch(API_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(data),
      });

      if (!response.ok) {
        throw new Error("Failed to send your message");
      }
      // comback to handle this properly with original error message
      const responseData = await response.json();
      setResponseMessage(responseData.message);
      return responseData;
    } catch (error) {
      setResponseMessage(
        "Failed to send your message. Please try again later.",
      );
      return error;
    } finally {
      setIsSubmitting(false);
    }
  };
};