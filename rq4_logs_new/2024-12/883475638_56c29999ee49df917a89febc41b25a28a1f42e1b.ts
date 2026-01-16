import { useState, useEffect } from "react";
import axios from "axios";
import { Deck, Flashcard } from "../types/FlashCardTypes";
import { v4 as uuidv4 } from "uuid";

const token = localStorage.getItem("token");

export const useFlashcardHooks = () => {
  const [flashcards, setFlashcards] = useState<Flashcard[]>([]);
  const [decks, setDecks] = useState<Deck[]>([]);
  const [isReviewing, setIsReviewing] = useState<boolean>(false);
  const [onFirstPage, setOnFirstPage] = useState<boolean>(true);
  const [deckTitle, setDeckTitle] = useState<string>("");
  const [deckId, setDeckId] = useState<string | null>(null);

  // Fetch decks and flashcards on mount
  useEffect(() => {
    const fetchDecks = async () => {
      try {
        if (!token) throw new Error("No token found");

        const response = await axios.get(`http://localhost:3002/cards/getDecks`, {
          headers: { Authorization: `Bearer ${token}` },
        });

        const deckData = response.data.map((deck: { deck_id: string; title: string }) => ({
          deck_id: deck.deck_id,
          title: deck.title,
        }));
        setDecks(deckData);
      } catch (error) {
        console.error("Error fetching decks:", error);
      }
    };

    const fetchFlashcards = async () => {
      try {
        const response = await axios.get(`http://localhost:3002/cards/getFlashcards`);
        const flashcardData = response.data.map((flashcard: {
          question: string;
          answer: string;
          flashcard_id: string;
          unique_flashcard_id: string;
        }) => ({
          question: flashcard.question,
          answer: flashcard.answer,
          flashcard_id: flashcard.flashcard_id,
          unique_flashcard_id: flashcard.unique_flashcard_id,
        }));
        setFlashcards(flashcardData);
      } catch (error) {
        console.error("Error fetching flashcards:", error);
      }
    };

    fetchDecks();
    fetchFlashcards();
  }, []);

  // Watch for deckId changes
  useEffect(() => {
    if (deckId) console.log(`Updated deckId: ${deckId}`);
  }, [deckId]);

  const saveDeck = async () => {
    if (!deckTitle.trim()) {
      alert("Deck title cannot be empty.");
      return;
    }

    try {
      setOnFirstPage(true);
      const data: Deck = { title: deckTitle, deck_id: uuidv4() };

      await axios.post(`http://localhost:3002/cards/insertDecks`, data, {
        headers: { Authorization: `Bearer ${token}` },
      });

      setDeckTitle("");
      alert("Deck saved successfully!");
    } catch (error) {
      console.error("Error saving deck:", error);
    }
  };

  const loadDeck = async (deck_id: string) => {
    try {
      setDeckId(deck_id);
      setIsReviewing(false);
      setOnFirstPage(false);

      const response = await axios.get(`http://localhost:3002/cards/getFlashcards`, {
        params: { deck_id },
      });

      const flashcardData = response.data.map((flashcard: {
        question: string;
        answer: string;
        flashcard_id: string;
        unique_flashcard_id: string;
      }) => ({
        question: flashcard.question,
        answer: flashcard.answer,
        flashcard_id: flashcard.flashcard_id,
        unique_flashcard_id: flashcard.unique_flashcard_id,
      }));

      setFlashcards(flashcardData);
    } catch (error) {
      console.error("Error loading deck:", error);
      setFlashcards([]);
    }
  };

  const deleteDeck = async (deckId: string) => {
    try {
      await axios.delete(`http://localhost:3002/cards/deleteDeck/${deckId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      setDecks(decks.filter((deck) => deck.deck_id !== deckId));
    } catch (error) {
      console.error("Error deleting deck:", error);
    }
  };

  const deleteFlashcard = async (unique_flashcard_id: string) => {
    try {
      await axios.delete(`http://localhost:3002/cards/deleteFlashcard/${unique_flashcard_id}`);
      setFlashcards(flashcards.filter((flashcard) => flashcard.unique_flashcard_id !== unique_flashcard_id));
      console.log("Flashcard deleted successfully!");
    } catch (error) {
      console.error("Error deleting flashcard:", error);
    }
  };

  return {
    flashcards,
    decks,
    isReviewing,
    onFirstPage,
    deckTitle,
    deckId,
    setDecks,
    setFlashcards,
    setDeckTitle,
    setDeckId,
    setIsReviewing,
    setOnFirstPage,
    saveDeck,
    loadDeck,
    deleteDeck,
    deleteFlashcard,
  };
};