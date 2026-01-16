import { red, blue } from '@material-ui/core/colors';
import { createTheme } from '@material-ui/core/styles';
// A custom theme for this app
const theme = createTheme({
    mixins: {
        toolbar: {
            height: 64,
        },
    },
    palette: {
        primary: {
            main: blue.A100, //'#556cd6',
        },
        secondary: {
            main: red.A400, //'#19857b',
            contrastText: '#ffffff',
        },
        background: {
            default: '#fff',
        },
    },
});

export default theme;