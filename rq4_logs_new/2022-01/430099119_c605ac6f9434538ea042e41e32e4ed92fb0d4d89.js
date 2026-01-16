import React from "react";
import { Routes, Route, Outlet } from "react-router-dom";

import { useParams } from "react-router-dom";
export default function Iphone() {
  let params = useParams();
  return (
    <div>
      {/* <Route path="iphone" element={<div>ddffdfd</div>}>
                <Route path=":12" element={<div>9</div>} />
                <Route path="sent" element={<div>10dff dfdfdff</div>} />
            </Route> */}
      iphone {params.iphone}
    </div>
  );
}