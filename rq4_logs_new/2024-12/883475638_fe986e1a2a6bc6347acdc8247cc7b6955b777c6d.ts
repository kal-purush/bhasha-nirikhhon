import { Request, Response } from "express";
import { pool, router } from "../database/CarmineDB";
import authenticateToken  from '../middleware/AuthMiddleware'


router.get("/getUser", authenticateToken, async (req: Request, res: Response) => {
    try {
        if (!req.user) {
            return res.status(401).json({ message: 'Unauthorized: No user information' });
          }
      
        const userId = req.user.user_id;

        const result = await pool.query("SELECT * FROM users WHERE user_id = $1", [userId]);

        res.status(200).json(result.rows[0]);
    } catch (error) {
      console.error("Error fetching users:", error);
      res.status(500).json({ message: "Error fetching users" });
    }
  });


export default router;