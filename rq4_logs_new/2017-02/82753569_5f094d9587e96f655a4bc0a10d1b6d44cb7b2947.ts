/* Dependencies */
import * as Sequelize from 'sequelize';

// Model Definition
function DefineUser(db) {
  const User = db.define('user', {
    email: { 
      type: Sequelize.STRING,
      allowNull: false,
      unique: true,
      validate: {
        isUnique: validateUniqueEmail
      }
    },

    firstName: { 
      type: Sequelize.STRING,
      allowNull: false
    },
     
    lastName: { 
      type: Sequelize.STRING,
      allowNull: false
    },

    organization: { 
      type: Sequelize.STRING
    },

    phoneNumber: { 
      type: Sequelize.STRING,
      allowNull: false
    },

    planID: { 
      type: Sequelize.STRING,
      allowNull: false
    },

    isAdmin: { 
      type: Sequelize.BOOLEAN,
      defaultValue: false
    } 
  })

  // Ensure email is unique
  function validateUniqueEmail(value, next) {
    User.find({ where: {email: value}})
        .then((user) => {
          if (user) {
            return next('Email address is already in use')
          }

          return next()
        })
        .catch((err) => {
          return next(err)
        })
  }

  return User
}


export default DefineUser