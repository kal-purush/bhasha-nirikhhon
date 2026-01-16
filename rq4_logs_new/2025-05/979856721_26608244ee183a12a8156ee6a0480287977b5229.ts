import { FormSection } from "@/schema/form-schema-type";
import axios from "axios";
import { handleApiError } from "./handle-api-error";

const SECTION_URL = `${process.env.NEXT_PUBLIC_BACKEND_BASE_URL}/form-section`;

export const updateSection = async (formId: string, sectionId: string, data: Partial<Omit<FormSection, "id" | "formId">>) => {
    try {
        const section = {...data, formId, sectionId }
        const response = await axios.put(SECTION_URL, section, {
            withCredentials: true,
        })

        console.log('section respone', response)
        return response.data
    } catch (error) {
        return handleApiError(error, "failed to update section") 
    }
}

export const createSection = async (formId: string, title: string) => {
    try {
        const response = await axios.post(SECTION_URL, { formId, title }, {
            withCredentials: true,
        }) 

        console.log('create section', response)
        return response.data
    } catch (error) {
        return handleApiError(error, "Failed to create section") 
    }
}