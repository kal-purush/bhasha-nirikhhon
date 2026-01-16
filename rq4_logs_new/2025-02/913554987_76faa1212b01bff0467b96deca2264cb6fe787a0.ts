import { handleApiError } from "./userDataApi";

const backendUrl = import.meta.env.VITE_BACKEND_URL;

export const postImages = async (formData: FormData) => {
    try {
        const response = await fetch(`${backendUrl}/api/reviewimage`, {
            method: "POST",
            body: formData
        });

        if (!response.ok) {
            throw new Error(`[Backend] Network error: ${response.status}`);
        }

        const imgUrlList: string[] = await response.json();
        return imgUrlList;
    }
    catch (error) {
        handleApiError(error);
    }
}