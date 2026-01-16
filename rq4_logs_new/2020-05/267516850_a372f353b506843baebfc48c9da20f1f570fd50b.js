import React from "react";
import { Link } from "react-router-dom";

import { makeStyles } from "@material-ui/core/styles";
import SwipeableDrawer from "@material-ui/core/SwipeableDrawer";
import IconButton from "@material-ui/core/IconButton";
import Toolbar from "@material-ui/core/Toolbar";
import MenuIcon from "@material-ui/icons/Menu";
import Typography from "@material-ui/core/Typography";
import List from "@material-ui/core/List";
import Divider from "@material-ui/core/Divider";
import ListItem from "@material-ui/core/ListItem";
import ListItemText from "@material-ui/core/ListItemText";
import ChevronLeftIcon from "@material-ui/icons/ChevronLeft";
import AccountCircle from "@material-ui/icons/AccountCircle";

import Home from "../../../assets/images/home.png";
import Cart from "../../../assets/images/cart.png";

const useStyles = makeStyles({
  list: {
    width: 250,
  },
  fullList: {
    width: "auto",
  },
  inputRoot: {
    color: "inherit",
  },
});

export default function Userheaderwithdrawerform(Props) {
  const classes = useStyles();
  const [state, setState] = React.useState({
    title: "",
    left: false,
  });

  const toggleDrawer = (side, open) => (event) => {
    if (
      event &&
      event.type === "keydown" &&
      (event.key === "Tab" || event.key === "Shift")
    ) {
      return;
    }

    setState({ ...state, [side]: open });
  };

  const sideList = (side) => (
    <div>
      <div
        className={classes.list}
        role="presentation"
        onClick={toggleDrawer(side, false)}
        onKeyDown={toggleDrawer(side, false)}
      >
        <div style={{ textAlign: "right" }} className={classes.drawerHeader}>
          <IconButton onClick={toggleDrawer("left", false)}>
            <ChevronLeftIcon />
          </IconButton>
        </div>
        <Divider />

        <Link to="/foodcart">
          <List style={{ color: "black" }}>
            {["Home"].map((text, index) => (
              <ListItem
                style={{
                  color: "black",
                  textAlign: "center",
                  paddingLeft: "20px",
                  paddingRight: "20px",
                  paddingTop: "12px",
                  paddingBottom: "12px",
                }}
                button
                key={text}
              >
                <img
                  alt={"home_icon"}
                  style={{ marginTop: -4, marginBottom: 0, width: "10%" }}
                  src={Home}
                />
                <ListItemText primary={text} />
              </ListItem>
            ))}
          </List>
        </Link>
        <Divider />

        <Link to="/foodcart/cart">
          <List style={{ color: "black" }}>
            {["Cart"].map((text, index) => (
              <ListItem
                style={{
                  color: "black",
                  textAlign: "center",
                  paddingLeft: "20px",
                  paddingRight: "20px",
                  paddingTop: "12px",
                  paddingBottom: "12px",
                }}
                button
                key={text}
              >
                <img
                  alt="FoodImage"
                  style={{ marginTop: -4, marginBottom: 0, width: "10%" }}
                  src={Cart}
                />
                <ListItemText primary={text} />
              </ListItem>
            ))}
          </List>
        </Link>
        <Divider />
      </div>
    </div>
  );

  return (
    <div>
      <Toolbar style={{ backgroundColor: "#2d73eb", color: "white" }}>
        <IconButton
          color="inherit"
          aria-label="Open drawer"
          onClick={toggleDrawer("left", true)}
          edge="start"
        >
          <MenuIcon />
        </IconButton>
        <Typography
          style={{
            width: "97%",
            backgroundColor: "#2d73eb",
            color: "white",
            marginLeft: 10,
          }}
          variant="h6"
          noWrap
        >
          {Props.title}
        </Typography>

        <IconButton
          edge="end"
          aria-label="Account of current user"
          aria-haspopup="true"
          color={"inherit"}
        >
          <AccountCircle />
        </IconButton>
      </Toolbar>
      <SwipeableDrawer
        open={state.left}
        onClose={toggleDrawer("left", false)}
        onOpen={toggleDrawer("left", true)}
      >
        {sideList("left")}
      </SwipeableDrawer>
    </div>
  );
}