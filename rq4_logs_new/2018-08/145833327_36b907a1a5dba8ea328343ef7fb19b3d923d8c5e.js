// Finds the energy expended moving up a distance
//
// Parameters:
//   distance - the horizontal distance in metres
//   changeInElevation - the change in elevation in metres
// Returns:
//   workDone in calories
function getEnergyExpenditure(distance, changeInElevation) {
	if (changeInElevation < 0) return 0;
	let mass = 75 + 8; // Assuming average human weight of 75kg and bike weight of 8kg

	// find the energy expended by elevation change
	let angle = (changeInElevation < 0) ? 0 : Math.atan(changeInElevation/distance);
	let gravityForce = 9.81 * mass * Math.sin(angle);

	// Find the energy expended by biking for that distance
	let normalForce = 9.81 * mass * Math.cos(angle);
	rollingResistanceForce = 0.02 * normalForce;

	totalThrustForce = rollingResistanceForce + gravityForce;
	distance = Math.sqrt(distance**2 + changeInElevation**2);
	workDone = totalThrustForce * distance * 0.001; // Joules/1000 = calories
	return workDone;
}