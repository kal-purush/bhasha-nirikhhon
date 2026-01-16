"use strict";

/* global moment */

/**
 * --------------------------------------------------------------------------
 * CoreUI Pro Boostrap Admin Template (2.1.14): advanced-forms.js
 * Licensed under MIT (https://coreui.io/license)
 * --------------------------------------------------------------------------
 */

/* eslint-disable no-magic-numbers */
$('#date').mask('99/99/9999');
$('#phone').mask('(999) 999-9999');
$('#tin').mask('99-9999999');
$('#ssn').mask('999-99-9999');
$('#eyescript').mask('~9.99 ~9.99 999');
$('#ccn').mask('9999 9999 9999 9999');
$('#select2-1, #select2-2, #select2-4').select2({
  theme: 'bootstrap'
});
$('#select2-3').select2({
  theme: 'bootstrap',
  placeholder: 'Your Favorite Football Team',
  allowClear: true
});

//# sourceMappingURL=advanced-forms.js.map