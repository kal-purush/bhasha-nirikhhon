import { Request, Response } from 'express';
import { pool, router } from '../database/CarmineDB';


router.get('/:user_id', async (req: Request, res: Response): Promise<Response> => {
  const { user_id } = req.params;

  try {
    // Query the database to retrieve the token for the given user_id
    const result = await pool.query(
      `SELECT token FROM users WHERE user_id = $1`,
      [user_id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Token not found for this user." });
    }

    const token: string = result.rows[0].token;
    return res.status(200).json({ token });

  } catch (error: unknown) {
    if (error instanceof Error) {
      console.error("Error retrieving token:", error.message);
      console.error("Stack trace:", error.stack);

      return res.status(500).json({
        message: "Internal server error.",
        error: error.message,
        stack: error.stack
      });
    } else {
      console.error("Unknown error:", error);
      return res.status(500).json({
        message: "Internal server error.",
        error: "An unknown error occurred."
      });
    }
  }
});

// New endpoint to handle logout
router.post('/logout/:user_id', async (req: Request, res: Response): Promise<Response> => {
  const { user_id } = req.params;

  try {
    const result = await pool.query(
      `UPDATE users SET token = NULL WHERE user_id = $1 RETURNING user_id`,
      [user_id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "User  not found." });
    }

    return res.status(200).json({ message: "Logged out successfully." });

  } catch (error: unknown) {
    console.error("Error during logout:", error);
    return res.status(500).json({ message: "Internal server error." });
  }
});

export default router;