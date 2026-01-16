"use client";

import { useSignIn, useSignUp } from "@clerk/nextjs";
import { useCallback, useEffect, useState } from "react";

/**
 * This utility helps diagnose and test OAuth authentication issues
 */
export const useOAuthTest = () => {
  const { signIn, isLoaded: isSignInLoaded } = useSignIn();
  const { signUp, isLoaded: isSignUpLoaded } = useSignUp();
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  // Test Google OAuth sign-in
  const testGoogleSignIn = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);
      setResult("Testing Google OAuth sign-in...");

      if (!isSignInLoaded) {
        throw new Error("Sign-in component not loaded yet");
      }

      // Log the available strategies
      console.log("Available first factors:", signIn?.supportedFirstFactors);

      // Specifically check for Google OAuth support
      const hasGoogleOAuth = signIn?.supportedFirstFactors?.some(
        (factor) => factor.strategy === "oauth_google"
      );

      if (!hasGoogleOAuth) {
        console.warn("oauth_google strategy not found in supported factors!");
        console.log(
          "Supported strategies:",
          signIn?.supportedFirstFactors?.map((f) => f.strategy).join(", ")
        );
      }

      // Using the correct strategy name - always use oauth_google
      await signIn?.authenticateWithRedirect({
        strategy: "oauth_google",
        redirectUrl: "/sso-callback",
        redirectUrlComplete: "/oauth-test",
      });

      setResult("Redirecting to Google OAuth...");
    } catch (err) {
      console.error("Google OAuth test error:", err);

      // Detailed error logging for debugging
      if (err instanceof Error) {
        // Save error to localStorage for persistence across redirects
        localStorage.setItem("oauth_error", err.message);

        // Check if this is the strategy error
        if (err.message.includes("strategy") || err.message.includes("google")) {
          setError(
            `Strategy Error: ${err.message}\n\nMake sure 'oauth_google' is used as the strategy name, not just 'google'.`
          );
        } else {
          setError(`Error: ${err.message}`);
        }
      } else {
        setError(`Unknown error: ${String(err)}`);
      }
    } finally {
      setIsLoading(false);
    }
  }, [signIn, isSignInLoaded]);

  // Test Facebook OAuth sign-in
  const testFacebookSignIn = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);
      setResult("Testing Facebook OAuth sign-in...");

      if (!isSignInLoaded) {
        throw new Error("Sign-in component not loaded yet");
      }

      // Log the available strategies
      console.log("Available first factors:", signIn?.supportedFirstFactors);

      // Using the correct strategy name
      await signIn?.authenticateWithRedirect({
        strategy: "oauth_facebook",
        redirectUrl: "/sso-callback",
        redirectUrlComplete: "/oauth-test",
      });

      setResult("Redirecting to Facebook OAuth...");
    } catch (err) {
      console.error("Facebook OAuth test error:", err);
      setError(`Error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsLoading(false);
    }
  }, [signIn, isSignInLoaded]);

  // Check for errors on page load
  useEffect(() => {
    // Parse URL params for OAuth errors
    const params = new URLSearchParams(window.location.search);
    if (params.has("error")) {
      const errorMessage = params.get("error_description") || params.get("error");
      setError(`OAuth Error: ${errorMessage}`);
    }

    // Check local storage for any saved errors
    const savedError = localStorage.getItem("oauth_error");
    if (savedError) {
      setError(`Previous Error: ${savedError}`);
      // Clear after displaying once
      localStorage.removeItem("oauth_error");
    }
  }, []);

  return {
    testGoogleSignIn,
    testFacebookSignIn,
    result,
    error,
    isLoading,
  };
};