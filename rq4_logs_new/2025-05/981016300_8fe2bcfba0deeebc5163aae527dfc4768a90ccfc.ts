import { useState } from "react";

// Hook for managing transaction state at the component level
export function useComponentTransactionState() {
  const [txConfirmed, setTxConfirmed] = useState(false);
  const [txHash, setTxHash] = useState<`0x${string}` | null>(null);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Reset all transaction state
  const resetTransactionState = () => {
    setTxConfirmed(false);
    setTxHash(null);
    setSuccess(false);
    setError(null);
  };

  return {
    txConfirmed,
    setTxConfirmed,
    txHash, 
    setTxHash,
    success,
    setSuccess,
    error,
    setError,
    resetTransactionState
  };
}