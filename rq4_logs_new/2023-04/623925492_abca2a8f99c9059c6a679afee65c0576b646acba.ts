import { createTheme} from "@mui/material/styles";

const heightButton='50px';
const colorButton="#7A7CFF";
const GlobalTheme = createTheme({
    components: {
        MuiButton: {
           styleOverrides:{
              root:({theme,ownerState})=>({
                ...(ownerState.variant==="outlined"&&{
                    height:heightButton,
                    color:colorButton,
                    borderColor:colorButton

                }),
                ...(ownerState.variant==="contained"&&{
                    height:heightButton,
                    backgroundColor:colorButton
                })
              }) 
              }
           
        },
    },
});

export default GlobalTheme;