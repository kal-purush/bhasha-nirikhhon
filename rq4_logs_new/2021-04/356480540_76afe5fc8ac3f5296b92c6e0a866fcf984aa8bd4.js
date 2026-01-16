import React, { useState } from "react";
import SideBar from "../Appointment/SideBar/SideBar";

const AddDoctor = () => {
  const [info, setInfo] = useState({});
  const [file, setFile] = useState(null);

  const handleBlur = (e) => {
    const newInfo = { ...info };
    newInfo[e.target.name] = e.target.value;
    setInfo(newInfo);
  };

  const handleFileChange = (e) => {
    console.log(e.target.files[0]);
    const newFile = e.target.files[0];
    setFile(newFile);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    const formData = new FormData();
    console.log(formData);
    formData.append("file", file);
    formData.append("name", info.name);
    formData.append("email", info.email);

    fetch("http://localhost:3002/addDoctor", {
      method: "POST",
      body: formData,
    })
      .then((response) => response.json())
      .then((data) => {
        console.log(data);
      })
      .catch((error) => {
        console.error(error);
      });
  };
  return (
    <div className="container-fluid">
      <div className="row">
        <div className="col-md-2">
          <SideBar />
        </div>

        <div className="col-md-8">
          <h2>Add Doctor</h2>

          <form  onSubmit={handleSubmit}>
            <div className="mb-3">
              <label for="email" className="form-label">
                Email address
              </label>
              <input
                onBlur={handleBlur}
                name="email"
                type="email"
                className="form-control"
                placeholder="name@example.com"
              />
            </div>

            <div className="mb-3">
              <label for="name" className="form-label">
                Email address
              </label>
              <input
                onBlur={handleBlur}
                name="name"
                type="text"
                className="form-control"
                placeholder="name"
              />
            </div>

            <div className="mb-3">
              <label for="formFileSm" className="form-label">
                Small file input example
              </label>
              <input
                onChange={handleFileChange}
                className="form-control form-control-sm"
                id="formFileSm"
                type="file"
              />
            </div>
            <button className="btn btn-info" type="submit">Submit</button>
          </form>
          
        </div>
      </div>
    </div>
  );
};

export default AddDoctor;