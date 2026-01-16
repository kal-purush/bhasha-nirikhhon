import { Context } from 'koa';

import Toy from './toy.model';
import User from '../users/user.model';
const { cloudinary } = require('../../utils/cloudinary');

export const getToy = async (ctx: Context) => {
  try {
    const { toyId } = ctx.params;
    const toy = await Toy.findById(toyId).populate('user');
    // check for any errors when retrieving the toys from the database
    ctx.assert(toy, 404, 'Toy not found.');
    // return success status code and JSON of the toy object
    ctx.status = 200;
    ctx.body = toy;
  } catch (err) {
    // send it to the error middleware
    throw err;
  }
};

export const getToyList = async (ctx: Context) => {
  try {
    const { sortPropName, sortOrder, filter } = ctx.query;

    //change the sort and filter properties depending on the sort and filter querystring
    // sort based on user preference, but if not given, sort based on latest date posted, followed by name

    let sortQuery: any = {}; //make the name sort ascendingly by default
    if (sortPropName && sortOrder) {
      // convert to a value that mongoose will accept
      let sortOrderNumber = sortOrder === 'asc' ? 1 : -1;
      // The order in which you assign parameters matters for its priority
      sortQuery[sortPropName as string] = sortOrderNumber;
      sortQuery.name = 1;
    } else {
      sortQuery = { date_posted: -1, name: 1 }; //default nested sort
    }

    let filterQuery = {};
    //filter based on frontend search bar value, but no filter if not given
    if (filter) {
      // make it ignore case
      const filterRegex = { $regex: new RegExp(filter as string, 'ig') };
      filterQuery = {
        $or: [
          { name: filterRegex },
          { franchise: filterRegex },
          { series: filterRegex },
          { manufacturer: filterRegex },
        ],
      };
    }

    const toys = await Toy.find(filterQuery)
      .collation({ locale: 'en', caseFirst: 'off' })
      .sort(sortQuery)
      .populate('user');
    // check for any errors when retrieving the toys from the database
    ctx.assert(toys, 500, 'Unable to retrieve the toys.');
    // return success status code and JSON of the toys array
    ctx.status = 200;
    ctx.body = toys;
  } catch (err) {
    throw err;
  }
};

export const createToy = async (ctx: Context) => {
  try {
    const { name, userId, manufacturer, description, franchise, series } =
      ctx.request.body;
    // check for any missing required properties
    ctx.assert(name, 400, 'A toy should have a name.');
    ctx.assert(userId, 401, 'User is not logged in. Please sign in.');

    // check if the user exists
    const user = await User.findById(userId);
    ctx.assert(user, 404, 'Unable to find user in the database.');

    // create MongoDB document and save it to database
    const newToy = new Toy({
      name: name.trim(),
      user: userId,
      manufacturer: manufacturer ? manufacturer.trim() : '',
      description: description ? description.trim() : '',
      franchise: franchise ? franchise.trim() : '',
      series: series ? series.trim() : '',
    });
    const savedToy: any = await newToy.save();
    ctx.assert(savedToy, 500, 'Failed to save the toy in the database.');
    // return success status code and JSON of the toy object
    ctx.status = 201;
    ctx.body = { ...savedToy._doc, user }; // ._doc contains the actual document properties
  } catch (err) {
    throw err;
  }
};

export const editToy = async (ctx: Context) => {
  try {
    const { name, userId, manufacturer, description, franchise, series } =
      ctx.request.body;
    const { toyId } = ctx.params;
    // check for any missing required properties
    ctx.assert(
      toyId,
      400,
      'The toyId is missing. Something went wrong with the client request.',
    );
    ctx.assert(name, 400, 'A toy should have a name.');
    ctx.assert(userId, 401, 'User is not logged in. Please sign in.');

    // look for the user in the database
    const user = await User.findById(userId);
    ctx.assert(user, 404, 'Unable to find user in the database.');

    // find MongoDB document, edit and save it to database, then check for any failures
    const toy = await Toy.findById(toyId);
    ctx.assert(toy, 404, 'Toy not found.');

    // remove whitespace, update the properties, and then save to the database
    toy.name = name.trim();
    toy.description = description ? description.trim() : '';
    toy.manufacturer = manufacturer ? manufacturer.trim() : '';
    toy.description = description ? description.trim() : '';
    toy.franchise = franchise ? franchise.trim() : '';
    toy.series = series ? series.trim() : '';
    const savedToy = await toy.save();
    ctx.assert(savedToy, 500, 'Failed to edit the toy.');

    // return success status code and JSON of the toy object + the user object
    ctx.status = 200;
    ctx.body = { ...savedToy._doc, user };
  } catch (err) {
    throw err;
  }
};

export const deleteToy = async (ctx: Context) => {
  try {
    const { userId } = ctx.request.body;
    const { toyId } = ctx.params;
    // check for missing properties
    ctx.assert(
      toyId,
      400,
      'The toyId is missing. Something went wrong with the client request.',
    );
    ctx.assert(userId, 401, 'User is not logged in. Please sign in.');

    // look for the user in the database
    const user = await User.findById(userId);
    ctx.assert(user, 404, 'Unable to find user in the database.');

    // find MongoDB document, remove it from the database, then check for any failures
    const toy = await Toy.findByIdAndRemove(toyId);
    ctx.assert(
      toy,
      404,
      'Unable to find and remove the toy from the database.',
    );
    // return success status code and JSON of the toy object
    ctx.status = 200;
    ctx.body = { toyId };
  } catch (err) {
    throw err;
  }
};

export const uploadToyImage = async (ctx: Context) => {
  try {
    const { fileStr, userId } = ctx.request.body;
    const { toyId } = ctx.params;

    ctx.assert(fileStr, 400, 'File string for image is missing.');
    ctx.assert(userId, 401, 'User is not logged in. Please sign in.');

    // look for the user in the database
    const user = await User.findById(userId);
    ctx.assert(user, 404, 'Unable to find user in the database.');
    const uploadedResponse = await cloudinary.uploader.upload(fileStr, {
      upload_preset: 'toy-gallery',
      public_id: `${toyId}-toy-main-image`,
    });

    const image_url = uploadedResponse.secure_url;
    const toy = await Toy.findById(toyId);
    toy.image_url = image_url;
    const savedToy = await toy.save();
    ctx.assert(savedToy, 500, 'Failed to update the toy.');

    ctx.body = { ...savedToy._doc, user };
  } catch (err) {
    throw err;
  }
};