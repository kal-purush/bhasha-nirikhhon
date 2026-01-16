console.log("App starting...");

import express from 'express';
import bodyParser from 'body-parser';
//import projectsRoutes from './routes/projects';
import reportsRoutes from './routes/reports';
import { authMiddleware } from './middleware/auth';
import projectRoutes from './routes/projectsRoutes';

const app = express();
const PORT = 3000;

// Middleware to parse JSON
app.use(bodyParser.json());

// Optional: Protect all routes with authentication middleware
// app.use(authMiddleware);

//app.use('/projects', projectsRoutes);
app.use('/reports', reportsRoutes);
app.use('/api', projectRoutes)

// Start the Express server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

export default app;