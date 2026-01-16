import { makeStyles } from '@material-ui/core/styles';

export const useStyles = makeStyles(theme => ({
    paper: {
        marginTop: theme.spacing(8),
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        padding: theme.spacing(2),
    },
    form: {
        width: '100%', // Fix IE 11 issue.
        marginTop: theme.spacing(1),
        display: 'contents',
    },
    submit: {
        margin: theme.spacing(3, 0, 2),
    },
    field: {
        margin: theme.spacing(3, 0, 0),
        width: '100%',
    },
}));