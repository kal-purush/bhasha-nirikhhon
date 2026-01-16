import apiMethods from "@/services/api/ApiMethods";

export function useRegisterNewUser(data) {
    return apiMethods.registerNewUser(data);
}