'use strict'
var request = require('browser-request')


var leases = (function () {

    var dhcp_widget = document.getElementById('dhcp')
    var dhcp_leases = document.getElementById('total-ip-leases')

    var fetch_data = function (url) {
        request(url, function (err, response, body) {
            if (err)
                throw err
            console.log(body)
            update_leases(body)
        })
    }

    var update_leases = function (amount) {
        dhcp_leases.innerHTML = amount
    }

    var runner = function (url) {
        fetch_data(url)
        window.setTimeout(function () {
            runner(url)
        }, 5000)
    }

    return {
        init: function () {
            var url = 'http://dolly.pp24.polarparty.no/cgi-bin/leases.cgi'
            runner(url)


        }
    }

})()

module.exports = leases