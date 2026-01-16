import React from "react";
import { Button } from "@mui/material";

const ButtonComponent = ({ label }) => {
  return (
    <Button
      type="submit"
      variant="contained"
      sx={{ backgroundColor: "#3D2CA7" }}
    >
      {label}
    </Button>
  );
};

export default ButtonComponent;