import React, { createContext, useContext, useState, ReactNode } from 'react';

// Define the type of the context value
interface DeckContextType {
  deckId: string | null;
  setDeckId: React.Dispatch<React.SetStateAction<string | null>>;
}

// Create the context with an undefined default value
const DeckContext = createContext<DeckContextType | undefined>(undefined);

// Define the type for the provider props
interface DeckProviderProps {
  children: ReactNode;
}

// Create the provider component
export const DeckProvider: React.FC<DeckProviderProps> = ({ children }) => {
  const [deckId, setDeckId] = useState<string | null>(null);

  return (
    <DeckContext.Provider value={{ deckId, setDeckId }}>
      {children}
    </DeckContext.Provider>
  );
};

// Create a custom hook to use the context
export const useDeck = (): DeckContextType => {
  const context = useContext(DeckContext);
  if (context === undefined) {
    throw new Error('useDeck must be used within a DeckProvider');
  }
  return context;
};