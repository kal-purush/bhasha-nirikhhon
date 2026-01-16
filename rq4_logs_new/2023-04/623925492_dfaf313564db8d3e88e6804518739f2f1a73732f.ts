import { createTheme } from "@mui/material/styles";
import COLORS from "../constants/CustomColors"
import SIZE from "../constants/CustomSize";
const GlobalTheme = createTheme({
    components: {
        MuiButton: {
            variants: [
                {
                    //green square button
                    props: { variant: "outlined", color: "success" },
                    style: {
                        height: SIZE.HEIGHT_TABLE_BUTTON,
                        width: SIZE.WIDTH_TABLE_BUTTON,
                        color: COLORS.SUCCESS,
                        borderColor: COLORS.SUCCESS,
                    },
                },
                {
                    //yellow square button
                    props: { variant: "outlined", color: "warning" },
                    style: {
                        height: SIZE.HEIGHT_TABLE_BUTTON,
                        width: SIZE.WIDTH_TABLE_BUTTON,
                        color: COLORS.WARNING,
                        borderColor: COLORS.WARNING,
                    },
                },
                {
                    // yellow rounded button with green boredr
                    props: { variant: "contained", color: "warning" },
                    style: {
                        height:SIZE.HEIGHT_ROUNDED_TABLE_BUTTON,
                        width: SIZE.WIDTH_ROUNDED_TABLE_BUTTON,
                        backgroundColor: COLORS.WARNING,
                        borderRadius: "20px",
                    },
                },
                {
                    // red rounded button with green boredr
                    props: { variant: "contained", color: "error" },
                    style: {
                        height:SIZE.HEIGHT_ROUNDED_TABLE_BUTTON,
                        width: SIZE.WIDTH_ROUNDED_TABLE_BUTTON,
                        backgroundColor:COLORS.ERROR,
                        borderRadius: "20px",
                    },
                },

                {
                    // green rounded button with green boredr
                    props: { variant: "contained", color: "success" },
                    style: {
                        height:SIZE.HEIGHT_ROUNDED_TABLE_BUTTON,
                        width: SIZE.WIDTH_ROUNDED_TABLE_BUTTON,
                        backgroundColor: COLORS.SUCCESS,
                        borderRadius: "20px",
                    },
                },
                {
                    //not avaible square button
                    props: { variant: "outlined", color: "info" },
                    style: {
                        height: SIZE.HEIGHT_TABLE_BUTTON,
                        width: SIZE.WIDTH_TABLE_BUTTON,
                        backgroundColor: COLORS.NOTAVAIBLE,
                        borderColor: COLORS.NOTAVAIBLE,
                        color: "white",
                    },
                },
                {
                    // white rounded button with black boreder
                    props: { variant: "outlined", color: "inherit" },
                    style: {
                        height:SIZE.HEIGHT_ROUNDED_TABLE_BUTTON,
                        width: SIZE.WIDTH_ROUNDED_TABLE_BUTTON,
                        backgroundColor: "white",
                        borderColor: "black",
                        borderRadius: "20px",
                        color: "black",
                    },
                },
            ],
            styleOverrides: {
                root: ({ theme, ownerState }) => ({
                    ...(ownerState.variant === "outlined" && {
                        height: SIZE.HEIGHT_ADMIN_BUTTON,
                        color: COLORS.ADMINCOLOR,
                        borderColor: COLORS.ADMINCOLOR,
                    }),
                    ...(ownerState.variant === "contained" && {
                        height: SIZE.HEIGHT_ADMIN_BUTTON,
                        backgroundColor:COLORS.ADMINCOLOR
                    }),
                }),
            },
        },
    },
});


export default GlobalTheme;