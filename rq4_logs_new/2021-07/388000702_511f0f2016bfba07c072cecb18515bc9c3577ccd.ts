import { Theme, makeStyles, createStyles } from '@material-ui/core/styles';

export const useStyles = makeStyles((theme: Theme) =>
    createStyles({
        item: {
            display: 'flex',
            flexDirection: 'row',
            flexWrap: 'nowrap',
            padding: theme.spacing(2, 4),
            borderBottom: `1px solid ${theme.palette.divider}`,
            maxWidth: theme.breakpoints.values.lg,
            transition: 'ease all 170ms',
            '&:hover': {
                boxShadow: `${theme.spacing(0, 2, 4, 0)} ${theme.palette.divider}`,
                transition: 'ease all 300ms',
                backgroundColor: '#fff',
                margin: theme.spacing(0, 0.6),
            },
        },
        avatar: {
            display: 'flex',
            padding: theme.spacing(0.6, 0.5, 0),
        },
        actions: {
            display: 'flex',
            flex: '1 0 auto',
            alignItems: 'center',
            justifyContent: 'flex-end',
            flexDirection: 'revert',
            flexWrap: 'nowrap',
        },
    })
);