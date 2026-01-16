import { makeStyles, Theme } from '@material-ui/core/styles';

const useStyles = makeStyles((theme: Theme) => ({
  fileDisplayContainer: {
    width: '100%',
    maxWidth: 200,
    display: 'block'
  },
  fileStatusBar: {
    width: '100%',
    verticalAlign: 'top',
    marginTop: 10,
    marginBottom: 20,
    position: 'relative',
    lineHeight: 50,
    height: 50
  },
  fileStatusBarDiv: {
    overflow: 'hidden'
  },
  fileType: {
    display: 'inline-block!important',
    position: 'absolute',
    fontSize: 12,
    fontWeight: 700,
    lineHeight: 13,
    marginTop: 25,
    padding: '0 4px',
    borderRadius: 2,
    boxShadow: '1px 1px 2px #abc',
    color: '#fff',
    background: '#0080c8',
    textTransform: 'uppercase'
  },
  fileName: {
    display: 'inline-block',
    verticalAlign: 'top',
    marginLeft: 50,
    color: '#4aa1f3'
  },
  fileTypeLogo: {
    width: 50,
    height: 50,
    position: 'absolute'
  },
  fileSize: {
    display: 'inline-block',
    verticalAlign: 'top',
    color: '#30693D',
    marginLeft: 10,
    marginRight: 5,
    fontWeight: 700,
    fontSize: 14
  },
  fileRemove: {
    position: 'absolute',
    top: 20,
    right: 10,
    lineHeight: 15,
    cursor: 'pointer',
    color: 'red',
    marginRight: -10
  }
}));

export { useStyles };