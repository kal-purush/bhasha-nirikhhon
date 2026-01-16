const func = require("../func");
const Customers = require('../models/Customers');

function controller() {

  function put(req, res) {

    try {
      const newCustomer = {
        email: data.email,
        siteID: req.siteID,
        created: new Date(),
        lastLogin: null,
        password: data.password,
        firstname: data.firstname,
        lastname: data.lastname,
        fullname: data.firstname + " " + data.lastname,
        company: data.company ? data.company : null,
        levelAuth: data.levelAuth ? data.levelAuth : 'EE',
        type: data.type ? data.type : null,
        GDPR: data.GDPR ? data.GDPR : false,
        personalDiscount: data.personalDiscount ? data.personalDiscount : 0,
        address: {
          town: data.town ? data.town : null,
          phone: data.phone ? data.phone : null,
          address: data.address ? data.address : null,
          address1: data.address1 ? data.address1 : null,
          country: data.country ? data.country : null,
          postcode: data.postcode ? data.postcode : null,
        }
      };

      const newEmployee = new Customers(newCustomer)
      newEmployee.save().then(result => {
        // logMSG({
        //   siteID: req.siteID,
        //   customerID: result._id,
        //   level: 'information',
        //   message: `New Customer was created with ID '${result._id}' for web site ID '${req.siteID}'`,
        //   sysOperation: 'create',
        //   sysLevel: 'customer'
        // });
        return func.createToken(res, result, siteData);
      }).catch((err) => {
        // logMSG({
        //   siteID: req.siteID,
        //   customerID: result._id,
        //   level: 'error',
        //   message: `New Customer was created with ID '${result._id}' for web site ID '${req.siteID}'`,
        //   sysOperation: 'create',
        //   sysLevel: 'customer'
        // });
        return res.status(500).send(err);
      });

    } catch (error) {
      return res.status(500).send(error);
    }
  }

  function patch(req, res) {
      const { employee } = req.employee;

      if (req.employee._id) delete req.employee._id

      Object.entries(req.body).forEach((item) => {
        const key = item[0];
        const value = item[1];
        employee[key] = value;
      });

      req.employee.save((err) => {
        if (err) return res.send(err);
        return res.json(employee)
      });
  }

  async function remove(req, res) {
    const by = {
      _id: req.params.id,
      siteID: req.siteID
    };

    const result = await Customers.deleteOne(by).exec();
    result.then((deletion) => {
      console.log('DELETE SELECTED => ', deletion);

      //   logMSG({ // Add new Log
      //     siteID: req.siteID,
      //     customerID: req.userId,
      //     level: 'information',
      //     message: `Customer with ID '${req.userId}' was successfully removed from web site with ID '${req.siteID}'.`,
      //     sysOperation: 'delete',
      //     sysLevel: 'customer'
      // });
      res.status(200).send(variables.successMsg.remove);

    }).catch((err) => {
      //   logMSG({ // Add new Log
      //     siteID: req.siteID,
      //     customerID: req.userId,
      //     level: 'error',
      //     message: func.onCatchCreateLogMSG(err),
      //     sysOperation: 'delete',
      //     sysLevel: 'customer'
      // });
      return res.status(500).send(err);
    });
  }

  return { put, patch, remove }
}

module.exports = controller;