export const listItemStyles = {
  "&:hover": {
    bgcolor: "primary.light",
    color: "primary.main",
    borderTopLeftRadius: 25,
    borderBottomLeftRadius: 25,
    "::before": {
      content: '""',
      position: "absolute",
      right: 0,
      top: "-50px",
      width: "50px",
      height: "50px",
      borderRadius: "50%",
      pointerEvents: "none",
      boxShadow: "35px 35px 0 10px white",
    },
    "::after": {
      content: '""',
      position: "absolute",
      right: 0,
      bottom: "-50px",
      width: "50px",
      height: "50px",
      borderRadius: "50%",
      pointerEvents: "none",
      boxShadow: "35px -35px 0 10px white",
    },
  },
  color: "primary.light",
};

export const listItemTextStyles = {
  display: { xs: "none", sm: "block" },
};

export const listStyles = {
  width: "100%",
  maxWidth: { xs: 70, sm: 200 },
  bgcolor: "primary.main",
  height: "100vh",
  pl: 1,
};

export const logoIconStyles = {
  color: "primary.light",
  mb: 4,
  mt: 1,
  ml: { xs: 1.5, sm: 0 },
  fontSize: 30,
};