import React, { useState } from "react";
import "../Components/Dashboard.css";
import { useNavigate } from "react-router-dom";
import axios from "axios";
const Form = () => {
  let navigate = useNavigate();
  const [formData, setFormData] = useState({
    name: "",
    number: "",
    date: "",
  });
  const handleSubmit = async (e) => {
    e.preventDefault();
    let response = await axios.post("http://localhost:5000/posts", formData);
    if (response) {
      alert("data submit successfuly");
      navigate("/home", { replace: true });
    } else {
      alert("something went wrong");
    }
    setFormData({
      name: "",
      number: "",
      date: "",
    });
  };
  return (
    <div className="main-Form">
      <form onSubmit={handleSubmit}>
        <div class="mb-3">
          <input
            required
            type="text"
            class="form-control"
            id="exampleFormControlInput1"
            value={formData.name}
            onChange={(e) => setFormData({ ...formData, name: e.target.value })}
            placeholder="Enter Name of Your Recruitement"
          />
        </div>
        <div className="form-row">
          <div className="form-group">
            <div className="row">
              <div className="col">
                <input
                  required
                  type="number"
                  className="form-control"
                  value={formData.number}
                  onChange={(e) =>
                    setFormData({ ...formData, number: e.target.value })
                  }
                  placeholder="Candidate No"
                />
              </div>
              <div className="col">
                <input
                  required
                  type="date"
                  className="form-control"
                  value={formData.date}
                  onChange={(e) =>
                    setFormData({ ...formData, date: e.target.value })
                  }
                  placeholder="Start Date"
                />
              </div>
            </div>
          </div>
        </div>
        <div class="mb-3">
          <textarea
            class="form-control"
            id="exampleFormControlTextarea1"
            rows="3"
            placeholder="write Description here"
          ></textarea>
        </div>
        {/* <div className="form-group">
          <input
            type="text"
            className="form-control"
            id="inputAddress"
            placeholder="1234 Main St"
          />
        </div>
        <div className="form-group">
          <input
            type="text"
            className="form-control"
            id="inputAddress2"
            placeholder="Apartment, studio, or floor"
          />
        </div> */}
        <div className="my-3  d-flex justify-content-end">
          <button
            className="btn btn-outline-success mx-2 rounded-0"
            value="reset"
            type="reset"
          >
            cancel
          </button>
          <button className="btn btn-success rounded-0" type="submit">
            Save & Continue
          </button>
        </div>
      </form>
    </div>

    // <div classNameName="main-Form">
    //   <form onSubmit={handleSubmit}>
    //     <div className="form-row">
    //       <div className="form-group">
    //         <input
    //           type="text"
    //           min="0"
    //           className="form-control"
    //           id="inputAddress2"
    //           value={formData.name}
    //           onChange={(e) =>
    //             setFormData({ ...formData, name: e.target.value })
    //           }
    //           placeholder="Enter Name of Your Recruitement"
    //         />
    //       </div>
    //       <div className="row">
    //         <div className="col">
    //           <input
    //             type="number"
    //             className="form-control"
    //             value={formData.number}
    //             onChange={(e) =>
    //               setFormData({ ...formData, number: e.target.value })
    //             }
    //             placeholder="Candidate No"
    //           />
    //         </div>
    //         <div className="col">
    //           <input
    //             type="date"
    //             className="form-control"
    //             value={formData.date}
    //             onChange={(e) =>
    //               setFormData({ ...formData, date: e.target.value })
    //             }
    //             placeholder="Start Date"
    //           />
    //         </div>
    //       </div>
    //     </div>
    //     {/* <div className="form-group">
    //       <input
    //         type="text"
    //         className="form-control"
    //         id="inputAddress"
    //         placeholder="1234 Main St"
    //       />
    //     </div>
    //     <div className="form-group">
    //       <input
    //         type="text"
    //         className="form-control"
    //         id="inputAddress2"
    //         placeholder="Apartment, studio, or floor"
    //       />
    //     </div> */}
    //     <div className="my-3  d-flex justify-content-end">
    //       <button
    //         className="btn btn-outline-success mx-2 rounded-0"
    //         type="submit"
    //       >
    //         cancel
    //       </button>
    //       <button className="btn btn-success rounded-0" type="submit">
    //         Save & Continue
    //       </button>
    //     </div>
    //   </form>
    // </div>
  );
};

export default Form;