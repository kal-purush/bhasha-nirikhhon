import React, { useState, useEffect } from "react";
import { MyMap } from "../Map";
import axios from "axios";

import { API_BASE_URL } from "../../Constants/apiConstants";

function submitForm(contentType, data, setResponse) {
  axios({
    url: `${API_BASE_URL}upload`,
    method: "POST",
    data: data,
    headers: {
      "Content-Type": contentType,
    },
  })
    .then((response) => {
      alert("successfully saved data");
      setResponse(response.data);
    })
    .catch((error) => {
      setResponse("error");
    });
}

let restrictedLocationsO = [];
function DataUpload() {

  

  return (
    <>
    </>
  );
}

export default DataUpload;