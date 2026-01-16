const express = require('express');
const router = express.Router();

const {getAppointmentsData,getMyDetails,volunteerdetails} = require('../controllers/familymember')

router.route("/getAppointmentsData").post(getAppointmentsData);
router.route("/getMyDetails").post(getMyDetails);
router.route("/volunteerdetails").post(volunteerdetails);

module.exports = router;