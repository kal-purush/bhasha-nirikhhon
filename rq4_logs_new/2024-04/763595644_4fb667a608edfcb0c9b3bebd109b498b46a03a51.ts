import { makeStyles } from 'tss-react/mui';

export const useFilterStyles = makeStyles({ name: 'HierarchyFilter' })(
  (theme) => ({
    root: {
      width: '100%',

      [theme.breakpoints.only('sm')]: {
        width: '80%',
      },
      [theme.breakpoints.up('md')]: {
        width: '60%',
      },
      transition: theme.transitions.create(['width', 'height'], {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.short,
      }),
    },
    input: {
      borderRadius: `${theme.spacing(2.5)} / 50%`,

      '& > input': {
        borderRadius: `${theme.spacing(2.5)} / 50%`,
        borderTopRightRadius: '0 !important',
        borderBottomRightRadius: '0 !important',
        padding: theme.spacing(1.5, 2.5),
      },
    },
  })
);

export const useHierarchyStyles = makeStyles({ name: 'HierarchyList' })(
  (theme) => ({
    root: {
      display: 'flex',
      flexDirection: 'column',
      gap: theme.spacing(1.5),
    },
    filter: {
      justifyContent: 'center',
      minHeight: 0,
      height: 'max-content',
    },
  })
);