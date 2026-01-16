/**
 * @swagger
 * /api/auth/login:
 *   post:
 *     summary: Logs in a user
 *     description: Logs in a user with username and password
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               username:
 *                 type: string
 *               password:
 *                 type: string
 *     responses:
 *       '200':
 *         description: Successful login
 *       '401':
 *         description: Invalid username or password
 *       '500':
 *         description: Internal server error
 */
app.post('/api/auth/login', async (req, res) => {
    // Your login logic here
  });
  