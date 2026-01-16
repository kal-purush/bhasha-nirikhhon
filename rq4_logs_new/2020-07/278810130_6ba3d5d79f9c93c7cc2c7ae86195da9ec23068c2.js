"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.fetchWeather = exports.welcome = void 0;
const Geocode_1 = __importDefault(require("../models/Geocode"));
const Weather_1 = __importDefault(require("../models/Weather"));
const responseFormatter_1 = __importDefault(require("../utils/responseFormatter"));
exports.welcome = (req, res) => {
    res.send('<h1>Welcome. This is a weather api</h1>');
};
exports.fetchWeather = (req, res, next) => {
    const { location } = req.query;
    const geocode = new Geocode_1.default(location);
    geocode.getGeocode
        .then((geoCodeRes) => {
        if (geoCodeRes instanceof Error) {
            next(geoCodeRes);
        }
        else {
            const { longitude, latitude, place } = geoCodeRes;
            const weather = new Weather_1.default(longitude, latitude, place);
            weather.getWeatherData.then((weatherRes) => {
                if (weatherRes instanceof Error) {
                    next(weatherRes);
                }
                responseFormatter_1.default(res, 200, 'Fetch successfully!', weatherRes);
            });
        }
    })
        .catch((error) => next(error));
};
//# sourceMappingURL=weather.js.map