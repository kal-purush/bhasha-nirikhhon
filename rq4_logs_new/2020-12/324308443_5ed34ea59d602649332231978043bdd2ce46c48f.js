import { getDataForLocation } from '../services/driver.service';


export const getAllDrivers = async (req, res, next) => {
    try {
        let { location, range = 200 } = req.body;

        if (location.lat || location.long) {
            let query = {
                location: { $geoWithin: { $centerSphere: [[location.long, location.lat], range / 3963.2] } }
            }
            let data = await getDataForLocation(query);
            return res.status(200).send({
                message: 'Success',
                data: data
            });
        } else {
            return res.status(422).send({
                message: 'payload is not correct'
            });
        }


    } catch (err) {
        return res.status(400).json({ message: 'Error' });
    }
}