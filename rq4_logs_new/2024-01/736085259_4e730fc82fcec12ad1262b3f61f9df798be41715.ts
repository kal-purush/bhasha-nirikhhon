import express from 'express';
import applicationController from '../controllers/applicationController'; // Adjust the import path based on your project structure

const router = express.Router();

// Submit an onboarding application
router.post('/submit', applicationController.submitApplication);

// Review an onboarding application
router.put('/review', applicationController.reviewApplication);

// Get application details
router.get('/details/:applicationId', applicationController.getApplicationDetails);

export default router;