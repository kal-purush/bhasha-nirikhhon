'use strict'
const request = require('request')

exports.handler = async event => {
  // TODO implement
  const email = event.email && event.email
  const password = event.password && event.password
  if (email && password)
    request.post(
      {
        url: 'http://dev2.tripapi.udrinkbackend.com/api/customer/history',
        form: {
          usermane: email,
          password: password
        }
      },
      function(err, httpResponse, body) {
        if (!err) {
          return {
            data: {
              httpResponse
            },
            message: 'Login successfully.',
            result: 1
          }
        } else {
          return {
            result: 0,
            message: 'The User Profile Service failed the logon'
          }
        }
      }
    )
  else
    return {
      result: 0,
      message: 'Incorrect Email or Password, Please try again.'
    }
}