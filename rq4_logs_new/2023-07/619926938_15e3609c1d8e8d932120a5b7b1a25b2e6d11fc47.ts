import {defineStore} from "pinia";
import {UserLoginDTO} from "~/types/user/userLoginDTO";
import {UserRegisterDTO} from "~/types/user/userRegisterDTO";
import {LoginResponse} from "~/types/loginResponse";
import {FetchError} from "ofetch";
import {Result} from "~/types/result";
import {UserVM} from "~/types/user/userVM";
import {RegisterResponse} from "~/types/registerResponse";


export const useUserStore = defineStore("user", () => {
    const globalStore = useGlobalStore();

    const user = ref<UserVM | null>(null);
    const computedUser = computed(() => user.value);
    const reset = () => {
        user.value = null;
    };

    const authenticate = async (loginCredential: UserLoginDTO): Promise<Result<void, string>> => {
        try {
            if (!loginCredential.email || !loginCredential.password) return {
                success: false,
                errorData: "Email o password mancanti"
            };

            const {user: userResp, accessToken} = await $fetch<LoginResponse>("/api/auth/login", {
                method: "POST",
                body: loginCredential
            })

            if (!accessToken) return {
                success: false,
                errorData: "Errore nel prendere access token"
            }
            if (!userResp.email || !userResp.id || !userResp.name || !userResp.lastName) return {
                success: false,
                errorData: "Qualcosa è andato storto nel prendere i dati dell'utente"
            }

            user.value = userResp;
            globalStore.setJwt(accessToken);

            return {
                success: true
            }
        } catch (error) {
            if (error instanceof FetchError) {
                console.error(`Errore in user.store.ts authenticate(), DATA: ${error.data}, statusCode: ${error.statusCode}, statusMessage: ${error.statusMessage}`)
                return {
                    success: false,
                    errorData: error.statusMessage
                }
            }
            console.error(`Errore non gestito user.store.ts authenticate() ${JSON.stringify(error, null, 4)}`)
            return {
                success: false,
                errorData: "Errore non gestito, riprovare più darti"
            }
        }
    };

    const authenticateViaSession = async (): Promise<Result<void, string>> => {
        try {
            const {user: userResp, accessToken} = await $fetch<LoginResponse>("/api/auth/session", {
                credentials: "include",
                headers: {
                    Authorization: globalStore.computedJwt
                        ? `Bearer ${globalStore.computedJwt}`
                        : "",
                },
            });

            if (!accessToken) return {
                success: false,
                errorData: "Errore nel prendere access token"
            }

            if (!userResp.email || !userResp.id || !userResp.name || !userResp.lastName) return {
                success: false,
                errorData: "Qualcosa è andato storto nel prendere i dati dell'utente"
            }

            user.value = userResp;
            globalStore.setJwt(accessToken);

            return {
                success: true
            }
        } catch (error) {
            if (error instanceof FetchError) {
                console.error(`Errore in user.store.ts authenticateViaSession(), DATA: ${error.data}, statusCode: ${error.statusCode}, statusMessage: ${error.statusMessage}`)
                return {
                    success: false,
                    errorData: error.statusMessage
                }
            }

            console.error(`Errore non gestito user.store.ts authenticateViaSession() ${JSON.stringify(error, null, 4)}`)
            return {
                success: false,
                errorData: "Errore non gestito, riprovare più darti"
            }
        }
    };

    const createUser = async (registerCredential: UserRegisterDTO): Promise<Result<void, string>> => {
        try {
            const {user: userResp, accessToken} = await $fetch<RegisterResponse>("/api/auth/register", {
                method: "POST",
                body: registerCredential
            })

            if (!accessToken) return {
                success: false,
                errorData: "Errore nel prendere access token"
            }

            if (!userResp.email || !userResp.id || !userResp.name || !userResp.lastName) return {
                success: false,
                errorData: "Qualcosa è andato storto nel prendere i dati dell'utente"
            }

            user.value = userResp;
            globalStore.setJwt(accessToken);

            return {
                success: true
            }
        } catch (error) {
            if (error instanceof FetchError) {
                console.error(`Errore in user.store.ts createUser(), DATA: ${error.data}, statusCode: ${error.statusCode}, statusMessage: ${error.statusMessage}`)
                return {
                    success: false,
                    errorData: error.statusMessage
                }
            }

            console.error(`Errore non gestito user.store.ts createUser() ${JSON.stringify(error, null, 4)}`)
            return {
                success: false,
                errorData: "Errore non gestito, riprovare più darti"
            }
        }
    };

    const endSession = async (): Promise<void> => {
        try {
            await $fetch("/api/auth/logout", {
                method: "POST",
                credentials: "include"
            })
        } catch (error) {
            if (error instanceof FetchError) {
                console.error(`Errore nel logout, DATA: ${JSON.stringify(error.data, null, 4)}`)
            } else {
                console.error(`Errore non gestito, errore -> ${JSON.stringify(error, null, 4)}`)
            }
        } finally {
            reset();
        }

    };

    const isLogged = computed(
        () => user.value !== null && user.value !== undefined
    );

    return {
        computedUser,
        authenticate,
        authenticateViaSession,
        reset,
        isLogged,
        createUser,
        endSession,
    };
});