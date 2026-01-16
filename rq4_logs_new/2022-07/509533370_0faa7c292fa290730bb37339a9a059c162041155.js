require('dotenv').config();

const Sequelize = require('sequelize');

const sequelizeConfig = require('../../database/config/config');
const httpStatus = require('../../utils/httpStatus');
const {
  Congregation, Address, District, City,
} = require('../../database/models');

const { NODE_ENV } = process.env;

const sequelize = new Sequelize(sequelizeConfig[NODE_ENV]);

const getAllCongregations = async () => {
  const congregations = await Congregation.findAll({
    include: [
      {
        model: Address,
        as: 'address',
        include: [{
          model: District,
          as: 'district',
          include: [{ model: City, as: 'city' }],
          attributes: { exclude: ['cityId'] },

        }],
        attributes: { exclude: ['districtId'] },
      },
    ],
    attributes: { exclude: ['addressId'] },
  });

  return congregations;
};

const getCongregationById = async (id) => {
  const congregation = await Congregation.findOne({
    where: { id },
    include: [
      {
        model: Address,
        as: 'address',
        include: [{
          model: District,
          as: 'district',
          include: [{ model: City, as: 'city' }],
          attributes: { exclude: ['cityId'] },

        }],
        attributes: { exclude: ['districtId'] },
      },
    ],
    attributes: { exclude: ['addressId'] },
  });

  if (!congregation) {
    const error = {
      status: httpStatus.NOT_FOUND,
      message: 'Congregation not found',
    };
    throw error;
  }

  return congregation;
};

const verifyIfExistsAddress = async (addressData) => {
  const address = await Address.findOne({
    where: { address: addressData.address },
  });

  if (address) {
    const error = {
      status: httpStatus.BAD_REQUEST,
      message: 'Address already exists',
    };
    throw error;
  }
};

const createCongregation = async (data) => {
  const {
    name, address, shepherd, region,
  } = data;

  try {
    const congregationCreated = sequelize.transaction(async (t) => {
      await verifyIfExistsAddress(address);

      const city = await City.findOne({
        where: { id: address.city.id },
      }, { transaction: t });

      const [district, created] = await District.findOrCreate({
        where: { name: address.district },
        defaults: { name: address.district, city: city.id },
      }, { transaction: t });

      const addressCreated = await Address.create({
        address: address.address,
        district: district ? district.id : created.id,
        number: address.number,
        complement: address.complement,
      }, { transaction: t });

      const congregation = await Congregation.create({
        name,
        address: addressCreated.id,
        shepherd,
        region,
      }, { transaction: t });

      return congregation;
    });

    return congregationCreated;
  } catch (er) {
    const error = {
      status: httpStatus.INTERNAL_SERVER_ERROR,
      message: er.message,
    };
    throw error;
  }
};

// const updateCongregation = async (data) => {
//   const {
//     id, name, address, shepherd, region,
//   } = data;

//   try {
//     const congregationUpdated = sequelize.transaction(async (t) => {
//       const congregationFound = await Congregation.findByPk(id, { transaction: t });

//       if (!congregationFound) {
//         const error = {
//           status: httpStatus.NOT_FOUND,
//           message: 'Congregation not found',
//         };
//         throw error;
//       }

//       const addressFound = await Address.findByPk(congregationFound.address, { transaction: t });

//       if (!addressFound) {
//         const error = {
//           status: httpStatus.NOT_FOUND,
//           message: 'Address not found',
//         };
//         throw error;
//       }

//       const city = await City.findOne({
//         where: { name: address.city },
//       }, { transaction: t });

//       const [district, created] = await District.findOrCreate({
//         where: { name: address.district },
//         defaults: { name: address.district, city: city.id },
//       }, { transaction: t });

//       await addressFound.update({
//         address: address.address,
//         district: district ? district.id : created.id,
//         number: address.number,
//         complement: address.complement,
//       }, { transaction: t });
//       await congregationFound.update({
//         name,
//         shepherd,
//         region,
//       }, { transaction: t });
//       return congregationFound;
//     });
//     return congregationUpdated;
//   } catch (er) {
//     const error = {
//       status: httpStatus.INTERNAL_SERVER_ERROR,
//       message: er.message,
//     };
//     throw error;
//   }
// };

module.exports = {
  getAllCongregations,
  getCongregationById,
  createCongregation,
};