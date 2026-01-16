import { useEffect, useCallback } from 'react';

type ActionFollowUpEvent = {
  detail: {
    message: string;
    source: string;
  };
};

/**
 * Hook to handle follow-up messages from action components
 * @param onFollowUpMessage Callback function that will be called when a follow-up message is received
 */
export function useActionFollowUp(
  onFollowUpMessage: (message: string, source: string) => void
) {
  const handleFollowUpMessage = useCallback(
    (event: CustomEvent<ActionFollowUpEvent['detail']>) => {
      const { message, source } = event.detail;
      if (message && message.trim() !== '') {
        onFollowUpMessage(message, source);
      }
    },
    [onFollowUpMessage]
  );

  useEffect(() => {
    // Cast is necessary because TypeScript's DOM lib doesn't know about our custom event
    const listener = (event: Event) => 
      handleFollowUpMessage(event as CustomEvent<ActionFollowUpEvent['detail']>);
    
    // Listen for the custom event
    window.addEventListener('action-followup-message', listener);
    
    // Cleanup
    return () => {
      window.removeEventListener('action-followup-message', listener);
    };
  }, [handleFollowUpMessage]);
}