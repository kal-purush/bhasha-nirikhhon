import express, { Request, Response } from 'express';
import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const router = express.Router();

const pool = new Pool({
    host: process.env.PG_HOST || "aws-0-ap-southeast-1.pooler.supabase.com",
    port: parseInt(process.env.PG_PORT || "6543"),
    database: process.env.PG_DATABASE || "postgres",
    user: process.env.PG_USER || "postgres.oizvoxoctozusoahxjos",
    password: process.env.PG_PASSWORD || "Carmine_123456789!!!",
});
 //>>>> declares server port running on 3002



// Get all decks
router.get('/decks', (req, res) => {
  res.json({ message: 'Decks endpoint is working' });
});

// Define the /currentFlashcards route
router.get('/currentFlashcards', (req, res) => {
  res.json({ message: 'Current flashcards endpoint is working' });
});

// Create a new deck
router.post("/decks", async (req, res) => {
  const { title } = req.body;
  const result = await pool.query("INSERT INTO decks (title) VALUES ($1) RETURNING *", [title]);
  res.json(result.rows[0]);
});

// Add a flashcard to a deck
router.post('/decks/:deckId/flashcards', async (req: Request, res: Response) => {
  const { deckId } = req.params;
  const { flashcards } = req.body;

  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const insertFlashcardQuery = `
        INSERT INTO flashcards (deck_id, question, answer)
        VALUES ($1, $2, $3)
      `;
      for (const flashcard of flashcards) {
        await client.query(insertFlashcardQuery, [deckId, flashcard.question, flashcard.answer]);
      }
      await client.query('COMMIT');
      res.status(201).json({ message: 'Flashcards created successfully' });
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Error creating flashcards:', error);
      res.status(500).json({ error: 'Failed to create flashcards' });
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Error connecting to the database:', error);
    res.status(500).json({ error: 'Database connection error' });
  }
});

// Delete a deck
router.delete("/decks/:deckId", async (req, res) => {
  const { deckId } = req.params;
  await pool.query("DELETE FROM decks WHERE id = $1", [deckId]);
  res.sendStatus(204);
});

// Delete a flashcard
router.delete("/flashcards/:flashcardId", async (req, res) => {
  const { flashcardId } = req.params;
  await pool.query("DELETE FROM flashcards WHERE id = $1", [flashcardId]);
  res.sendStatus(204);
});

router.put('/decks/:deckId/flashcards', async (req, res) => {
  const { deckId } = req.params;
  const { flashcards } = req.body;

  try {
      // Update flashcards for the specified deck in the database
      await pool.query('UPDATE decks SET flashcards = $1 WHERE id = $2', [JSON.stringify(flashcards), deckId]);
      res.status(200).send("Flashcards updated successfully");
  } catch (error) {
      console.error("Error updating flashcards:", error);
      res.status(500).send("Internal Server Error");
  }
});

export default router;