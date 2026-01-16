const pixelToRem = (size: number) => `${size / 16}rem`;

// fontSize
const fontSizes = {
  small: pixelToRem(14), // 0.8rem
  base: pixelToRem(16), // 1rem
  lg: pixelToRem(18),
  xl: pixelToRem(20),
  xxl: pixelToRem(22),
  xxxl: pixelToRem(24),
  titleSize: pixelToRem(50), //3.1rem
};

const deviceSizes = {
  mobileS: "320px",
  mobileM: "375px",
  mobileL: "700px",
  tablet: "820px",
  tabletL: "1024px",
};

const device = {
  mobileS: `only screen and (max-width: ${deviceSizes.mobileS})`,
  mobileM: `only screen and (max-width: ${deviceSizes.mobileM})`,
  mobileL: `only screen and (max-width: ${deviceSizes.mobileL})`,
  tablet: `only screen and (max-width: ${deviceSizes.tablet})`,
  tabletL: `only screen and (max-width: ${deviceSizes.tabletL})`,
};

const colors = {
  black: "#000000",
  gray: "#bcbcbc",
  green: "#3cb46e",
  blue: "#8c80ff",
  background: "rgba(77, 77, 77, 0.838)",
};

const common = {
  defaultButton: `
    background : #8c80ff;
    color : white;
    padding: 5px 20px 5px 20px;
    border: none;
    border-radius : 20px;
    outline:0px;
	cursor:pointer;
    text-decoration: none;
    `,
};

const theme = { fontSizes, colors, common, device, deviceSizes };

export default theme;