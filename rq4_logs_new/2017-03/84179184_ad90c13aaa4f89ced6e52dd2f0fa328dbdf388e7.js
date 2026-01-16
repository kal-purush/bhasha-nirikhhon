/* =====================================================================================
 * @author Stanislav Stepanenko
 * =====================================================================================
 * Copyright (c) 2015-2017 Rakuten Marketing
 * Licensed under MIT (https://github.com/linkshare/plus.garden.webdriver/blob/master/LICENSE)
 * ===================================================================================== */

module.exports = function (container) {
    container.register('Webdriver.Browser', require('./services/Browser.js'));
}