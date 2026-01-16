"use strict";

var customerReg = function () {
    // Private variables
    var formID;
    var form;
    var submitButton;
    var validation;

    // Private functions
    var initValidation = function () {
        validation = FormValidation.formValidation(
            form,
            {
                fields: {
                    fullname: {
                        validators: {
                            notEmpty: {
                                message: 'Please enter your name'
                            }
                        }
                    },
                    password: {
                        validators: {
                            notEmpty: {
                                message: 'A password is required to access your data at anytime'
                            }
                        }
                    },
                    phone: {
                        validators: {
                            notEmpty: {
                                message: 'Contact phone number is required'
                            },
                            numeric: {
                                message: 'This is not a valid number'
                            }
                        }
                    },
                    email: {
                        validators: {
							notEmpty: {
								message: 'Email address is required'
							},
                            emailAddress: {
								message: 'This is not a valid email address'
							}
						}
					}
                },
                plugins: {
                    trigger: new FormValidation.plugins.Trigger(),
                    submitButton: new FormValidation.plugins.SubmitButton(),
                    //defaultSubmit: new FormValidation.plugins.DefaultSubmit(), // Uncomment this line to enable normal button submit after form validation
                    bootstrap: new FormValidation.plugins.Bootstrap5({
                        rowSelector: '.fv-row',
                        eleInvalidClass: '',
                        eleValidClass: ''
                    })
                }
            }
        );

    }

    var handleForm = function () {
        submitButton.addEventListener('click', function (e) {
            e.preventDefault();

            validation.validate().then(function (status) {
                if (status == 'Valid') {
                    
                    AJAXcall(formID, submitButton, 'POST', 'controllers/users.php?type=customer', null, (responseMsg)=>{
                        if(responseMsg.status !== 'success'){
                          swal_Popup(responseMsg.status, responseMsg.message);
                          return false; 
                        }
                        swal_confirm("Yeah! We have saved your information. There\'s just one more step", "Login to add your measurements", "", false, 'success')
                        .then((result) => {
                            if (result.isConfirmed) {
                                var cid = responseMsg.data.user_id;
                                goTo('new?login&cid='+cid);

                            } else if (result.isDenied) {
                                console.log("Canceled");
                            }
                        })
                        .catch((error) => {
                            console.error(error);
                        });
                    });
                    
                } else {
                    swal_Popup('error', 'Sorry, some errors were detected, please try again.', 'Try Again!');
                }
            });
        });
    }

    // Public methods
    return {
        init: function () {
            formID = '#new_customer_reg';
            form = document.querySelector(formID);
            submitButton = form.querySelector('#new_customer_submit');

            initValidation();
            handleForm();
        }
    }
}();

// On document ready
KTUtil.onDOMContentLoaded(function() {
    customerReg.init();
});