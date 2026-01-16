/*
- This file defines a custom React hook to show toast notifications.
- It uses the useToast hook from Chakra UI for toast management.
- The showToast function displays a toast with title, description, and status.
- Toasts have a fixed duration of 3000 milliseconds and can be manually closed.
*/

// Importing necessary hooks and functions from React and Chakra UI
import { useToast } from "@chakra-ui/toast";
import { useCallback } from "react";

// Definition of the custom hook useShowToast
const useShowToast = () => {
	// Utilizing the useToast hook from Chakra UI to manage toast notifications
	const toast = useToast();

	// Using useCallback to memorize the showToast function to prevent unnecessary re-renders
	const showToast = useCallback(
		(title, description, status) => {
			// Invoking the toast function with an object containing the toast properties
			toast({
				title, // Title of the toast
				description, // Description text of the toast
				status, // Status type (e.g., "error", "success", "info", "warning")
				duration: 3000, // Toast visibility duration in milliseconds
				isClosable: true, // Whether the toast can be closed manually
			});
		},
		[toast] // Dependencies of useCallback, re-memorize if `toast` changes
	);

	// The hook returns the memorized showToast function
	return showToast;
};

// Exporting the useShowToast hook for use in other parts of the application
export default useShowToast;