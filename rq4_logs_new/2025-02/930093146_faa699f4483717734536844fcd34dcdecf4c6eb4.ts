import { create } from "zustand";
import { Session } from "@supabase/supabase-js";
import { supabase } from "@/lib/supabase";
import { showToast } from "@/lib/toast";
import { AuthState, UserResponse } from "@/types/auth";
import { authConfig } from "@/lib/auth-config";

let authAttempts = 0;
const maxAttempts = authConfig.rateLimiting.maxAttempts;
const windowSeconds = authConfig.rateLimiting.windowSeconds;
let lastAttemptTime = 0;

export const useAuthStore = create<AuthState>((set, get) => ({
  session: null,
  loading: true,
  error: null,
  initialized: false,
  rememberMe: localStorage.getItem("rememberMe") === "true",

  setSession: (session) => set({ session }),

  setRememberMe: (remember) => {
    localStorage.setItem("rememberMe", remember.toString());
    set({ rememberMe: remember });
  },

  signIn: async (email, password) => {
    try {
      // Rate limiting check
      const now = Date.now();
      if (now - lastAttemptTime < windowSeconds * 1000) {
        authAttempts++;
        if (authAttempts >= maxAttempts) {
          const resetTime = new Date(lastAttemptTime + windowSeconds * 1000);
          throw new Error(
            `Too many login attempts. Please try again after ${resetTime.toLocaleTimeString()}`,
          );
        }
      } else {
        authAttempts = 1;
        lastAttemptTime = now;
      }

      // Network check
      if (!navigator.onLine) {
        throw new Error(
          "No internet connection. Please check your network and try again.",
        );
      }

      set({ loading: true, error: null });
      const { data, error } = await supabase.auth.signInWithPassword({
        email,
        password,
      });

      if (error) {
        // Handle specific error cases
        if (error.message.includes("Invalid login")) {
          throw new Error("Invalid email or password");
        } else if (error.message.includes("Email not confirmed")) {
          throw new Error("Please verify your email address before signing in");
        } else if (error.message.includes("User not found")) {
          throw new Error("No account found with this email");
        }
        throw error;
      }

      // Reset attempts on successful login
      authAttempts = 0;
      set({ session: data.session });
      showToast.success("Successfully signed in");
      return data;
    } catch (error) {
      set({ error: error.message });
      showToast.error(error.message);
      throw error;
    } finally {
      set({ loading: false });
    }
  },

  signOut: async () => {
    try {
      if (!navigator.onLine) {
        throw new Error(
          "No internet connection. Your session will be cleared locally.",
        );
      }

      const { error } = await supabase.auth.signOut();
      if (error) throw error;

      // Clear local session data
      set({ session: null });
      localStorage.removeItem("rememberMe");
      showToast.success("Successfully signed out");
    } catch (error) {
      set({ error: error.message });
      showToast.error(error.message);

      // Force local signout on persistent errors
      set({ session: null });
    }
  },

  resetPassword: async (email) => {
    try {
      if (!navigator.onLine) {
        throw new Error(
          "No internet connection. Please check your network and try again.",
        );
      }

      set({ loading: true, error: null });
      const { error } = await supabase.auth.resetPasswordForEmail(email, {
        redirectTo: `${window.location.origin}/auth/reset-password`,
      });

      if (error) {
        if (error.message.includes("User not found")) {
          throw new Error("No account found with this email address");
        }
        throw error;
      }

      showToast.success("Password reset email sent");
    } catch (error) {
      set({ error: error.message });
      showToast.error(error.message);
    } finally {
      set({ loading: false });
    }
  },

  updatePassword: async (password) => {
    try {
      if (!navigator.onLine) {
        throw new Error(
          "No internet connection. Please check your network and try again.",
        );
      }

      set({ loading: true, error: null });
      const { error } = await supabase.auth.updateUser({ password });

      if (error) {
        if (error.message.includes("stronger password")) {
          throw new Error(
            "Please use a stronger password. It should include numbers, special characters, and uppercase letters.",
          );
        }
        throw error;
      }

      showToast.success("Password updated successfully");
    } catch (error) {
      set({ error: error.message });
      showToast.error(error.message);
    } finally {
      set({ loading: false });
    }
  },

  initialize: async () => {
    if (get().initialized) return;

    try {
      set({ loading: true, error: null });

      if (!navigator.onLine) {
        throw new Error(
          "No internet connection. Using cached session if available.",
        );
      }

      // Get initial session
      const {
        data: { session },
        error,
      } = await supabase.auth.getSession();

      if (error) throw error;

      set({ session, initialized: true });

      // Listen for auth changes
      supabase.auth.onAuthStateChange((_event, session) => {
        set({ session });
      });
    } catch (error) {
      set({
        error: error.message,
        initialized: true, // Mark as initialized even on error
        session: null, // Clear session on critical errors
      });
      showToast.error(error.message);
    } finally {
      set({ loading: false });
    }
  },
}));