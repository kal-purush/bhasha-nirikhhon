// helper function to calculate the distance between 2 given long lat
export function calculateDistance(
    lat1: number,
    lon1: number,
    lat2: number,
    lon2: number
) : number {
    const earthRadius = 6371; // the earth's radius in kilometers

    // Convert lat and lon from degrees to radians
    const lat1Rad = (lat1 * Math.PI) / 180;
    const lon1Rad = (lon1 * Math.PI) / 180;
    const lat2Rad = (lat2 * Math.PI) / 180;
    const lon2Rad = (lon2 * Math.PI) / 180;

    const latDiff = lat2Rad - lat1Rad;
    const lonDiff = lon2Rad - lon1Rad;

    // Haversine formula
    const a = Math.sin(latDiff / 2) ** 2 + Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.sin(lonDiff / 2) ** 2;
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

    // Calculate the distance
    const distance = earthRadius * c; // Result in km

    return distance;
}