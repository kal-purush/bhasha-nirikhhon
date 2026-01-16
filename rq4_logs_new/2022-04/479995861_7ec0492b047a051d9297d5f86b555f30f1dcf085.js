import React from "react";
import Box from "@mui/material/Box";
import BottomNavigation from "@mui/material/BottomNavigation";
import BottomNavigationAction from "@mui/material/BottomNavigationAction";
import RestoreIcon from "@mui/icons-material/Restore";
import FavoriteIcon from "@mui/icons-material/Favorite";
import LocationOnIcon from "@mui/icons-material/LocationOn";

import DashboardIcon from "@mui/icons-material/Dashboard";
import HomeIcon from "@mui/icons-material/Home";
import CreditCardIcon from "@mui/icons-material/CreditCard";
import BarChartIcon from "@mui/icons-material/BarChart";
import PersonIcon from "@mui/icons-material/Person";
import { useRouter } from "next/router";

const BottomNavi = ({ setCurrentPage }) => {
  const router = useRouter();
  const [value, setValue] = React.useState(0);

  return (
    <Box sx={{ width: "100%" }}>
      <BottomNavigation
        // showLabels
        value={value}
        onChange={(event, newValue) => {
          setValue(newValue);
        }}
        sx={{
          "& MuiBottomNavigationAction, .Mui-selected": {
            color: "#008037",
          },
        }}
      >
        <BottomNavigationAction
          onClick={() => setCurrentPage("dashboard")}
          label=""
          icon={<HomeIcon />}
        />
        <BottomNavigationAction
          onClick={() => setCurrentPage("card")}
          label=""
          icon={<CreditCardIcon />}
        />
        <BottomNavigationAction
          onClick={() => setCurrentPage("graph")}
          label=""
          icon={<BarChartIcon />}
        />
        <BottomNavigationAction
          onClick={() => setCurrentPage("profile")}
          label=""
          icon={<PersonIcon />}
        />
      </BottomNavigation>
    </Box>
  );
};

export default BottomNavi;