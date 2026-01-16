import { Database } from 'sqlite3';
import express from 'express';
import bodyParser from 'body-parser';
import logger from './logger';
import ValidationError from './utils/errors/ValidationError';
import ServerError from './utils/errors/ServerError';
import NotFoundError from './utils/errors/NotFoundError';

const app = express();
const jsonParser = bodyParser.json();

export default (db: Database) => {
  const logError = (error: { message: string, errorCode: string }) => logger.error({ ...error });

  app.get('/health', (req, res) => res.send('Healthy'));

  app.post('/rides', jsonParser, (req, res) => {
    const startLatitude = Number(req.body.start_lat);
    const startLongitude = Number(req.body.start_long);
    const endLatitude = Number(req.body.end_lat);
    const endLongitude = Number(req.body.end_long);
    const riderName = req.body.rider_name;
    const driverName = req.body.driver_name;
    const driverVehicle = req.body.driver_vehicle;

    if (
      startLatitude < -90 ||
      startLatitude > 90 ||
      startLongitude < -180 ||
      startLongitude > 180
    ) {
      const error = new ValidationError('Start latitude and longitude must be between -90 - 90 and -180 to 180 degrees respectively');
      logError(error);
      return res.status(400).send(error);
    }

    if (
      endLatitude < -90 ||
      endLatitude > 90 ||
      endLongitude < -180 ||
      endLongitude > 180
    ) {
      const error = new ValidationError('End latitude and longitude must be between -90 - 90 and -180 to 180 degrees respectively');
      logError(error);
      return res.status(400).send(error);
    }

    if (typeof riderName !== 'string' || riderName.length < 1) {
      const error = new ValidationError('Rider name must be a non empty string');
      logError(error);
      return res.status(400).send(error);
    }

    if (typeof driverName !== 'string' || driverName.length < 1) {
      const error = new ValidationError('Driver name must be a non empty string');
      logError(error);
      return res.status(400).send(error);
    }

    if (typeof driverVehicle !== 'string' || driverVehicle.length < 1) {
      const error = new ValidationError('Vehicle name must be a non empty string');
      logError(error);
      return res.status(400).send(error);
    }

    const values = [
      req.body.start_lat,
      req.body.start_long,
      req.body.end_lat,
      req.body.end_long,
      req.body.rider_name,
      req.body.driver_name,
      req.body.driver_vehicle,
    ];

    db.run(
      'INSERT INTO Rides(startLat, startLong, endLat, endLong, riderName, driverName, driverVehicle) VALUES (?, ?, ?, ?, ?, ?, ?)',
      values,
      function (err) {
        if (err) {
          const error = new ServerError('Unknown error');
          logError(error);
          return res.status(500).send(error);
        }

        db.all(
          'SELECT * FROM Rides WHERE rideID = ?',
          this.lastID,
          function (err, rows) {
            if (err) {
              const error = new ServerError('Unknown error');
              logError(error);
              return res.status(500).send(error);
            }

            res.send(rows);
          },
        );
      },
    );
  });

  app.get('/rides', (req, res) => {
    db.all('SELECT * FROM Rides', function (err, rows) {
      if (err) {
        const error = new ServerError('Unknown error');
        logError(error);
        return res.status(500).send(error);
      }

      if (rows.length === 0) {
        const error = new NotFoundError('Could not find any rides');
        logError(error);
        return res.status(404).send(error);
      }

      res.send(rows);
    });
  });

  app.get('/rides/:id', (req, res) => {
    db.all(
      `SELECT * FROM Rides WHERE rideID='${req.params.id}'`,
      function (err, rows) {
        if (err) {
          const error = new ServerError('Unknown error');
          logError(error);
          return res.status(500).send(error);
        }

        if (rows.length === 0) {
          const error = new NotFoundError('Could not find any rides');
          logError(error);
          return res.status(404).send(error);
        }

        res.send(rows);
      },
    );
  });

  return app;
};