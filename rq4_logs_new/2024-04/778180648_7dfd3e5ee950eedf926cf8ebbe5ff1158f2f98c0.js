import React, { useState, useRef } from "react";
import { Modal, Button } from "react-bootstrap";

const MyForm = ({ setShowModal, showModal }) => {
  const [formData, setFormData] = useState({
    firstName: "",
    lastName: "",
    phoneNumber: "",
    city: "",
    state: "",
  });

  const [errors, setErrors] = useState({
    firstName: "",
    lastName: "",
    phoneNumber: "",
    city: "",
    state: "",
  });

  const formRef = useRef(null);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData((prevData) => ({
      ...prevData,
      [name]: value,
    }));
  };

  const submitForm = () => {
    console.log(formRef.current);

    if (formRef.current) {
      formRef.current.submit();
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();

    const validationErrors = validateForm(formData);
    if (Object.keys(validationErrors).length === 0) {
      setShowModal(false);
    } else {
      setErrors(validationErrors);
    }
    console.log(validationErrors);
  };

  const validateForm = (data) => {
    let errors = {};
    if (!data.firstName) {
      errors.firstName = "First name is required";
    }
    if (!data.lastName) {
      errors.lastName = "Last name is required";
    }
    if (!data.phoneNumber) {
      errors.phoneNumber = "Phone number is required";
    }
    if (!data.city) {
      errors.city = "City is required";
    }
    if (!data.state) {
      errors.state = "State is required";
    }

    return errors;
  };

  return (
    <Modal show={showModal} onHide={() => setShowModal(false)}>
      <Modal.Header closeButton>
        <Modal.Title>Your Data</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        <div className="row justify-content-center">
          <div className="col-md-8">
            <form
              id=""
              className="row g-3"
              ref={formRef}
              onSubmit={handleSubmit}
            >
              <div className="col-md-12">
                <label htmlFor="validationServer01" className="form-label">
                  First name
                </label>
                <input
                  type="text"
                  className="form-control  custom-form-control"
                  id="validationServer01"
                  value={formData.firstName}
                  onChange={handleChange}
                  name="firstName"
                  required
                />
                {errors.firstName && (
                  <div className="invalid-feedback">{errors.firstName}</div>
                )}
              </div>
              <div className="col-md-12">
                <label htmlFor="validationServer02" className="form-label">
                  Last name
                </label>
                <input
                  type="text"
                  className="form-control  custom-form-control"
                  id="validationServer02"
                  value={formData.lastName}
                  onChange={handleChange}
                  name="lastName"
                  required
                />
                <div className="valid-feedback">Looks good!</div>
              </div>
              <div className="col-md-12">
                <label
                  htmlFor="validationServerUsername"
                  className="form-label"
                >
                  Phone Number
                </label>
                <div className="input-group  custom-form-control">
                  <input
                    type="phone"
                    className="form-control "
                    id="validationServerUsername"
                    value={formData.phoneNumber}
                    aria-describedby="inputGroupPrepend3 validationServerUsernameFeedback"
                    onChange={handleChange}
                    name="phoneNumber"
                    required
                  />
                  <div
                    id="validationServerUsernameFeedback"
                    className="invalid-feedback"
                  >
                    Please enter your a number.
                  </div>
                </div>
              </div>
              <div className="col-md-12">
                <label htmlFor="validationServer03" className="form-label">
                  City
                </label>
                <input
                  type="text"
                  className="form-control  custom-form-control"
                  id="validationServer03"
                  aria-describedby="validationServer03Feedback"
                  onChange={handleChange}
                  value={formData.city}
                  name="city"
                  required
                />
                <div
                  id="validationServer03Feedback"
                  className="invalid-feedback"
                >
                  Please provide a valid city.
                </div>
              </div>
              <div className="col-md-12">
                <label htmlFor="validationServer04" className="form-label">
                  State
                </label>
                <select
                  className="form-select  custom-form-control"
                  id="validationServer04"
                  aria-describedby="validationServer04Feedback"
                  onChange={handleChange}
                  value={formData.state}
                  name="state"
                  required
                >
                  <option disabled value="">
                    Choose...
                  </option>
                  <option>Delivery</option>
                  <option>To Go</option>
                </select>
                <div
                  id="validationServer04Feedback"
                  className="invalid-feedback"
                >
                  Please select a valid state.
                </div>
              </div>
            </form>
          </div>
        </div>
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" onClick={() => setShowModal(false)}>
          Close
        </Button>
        <Button variant="primary" onClick={() => submitForm()}>
          Save
        </Button>
      </Modal.Footer>
    </Modal>
  );
};

export default MyForm;