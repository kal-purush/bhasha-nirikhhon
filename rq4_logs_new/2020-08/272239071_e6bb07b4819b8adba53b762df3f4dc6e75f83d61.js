import React, {useState, useEffect} from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Paper from '@material-ui/core/Paper'
import Button from '@material-ui/core/Button'
import Typography from '@material-ui/core/Typography';
import Grid from '@material-ui/core/Grid';
import TextField from '@material-ui/core/TextField';
import MenuItem from '@material-ui/core/MenuItem';
import theme from '../styles/theme'
import ATApi from './axios.service';
import Close from '../closeButton'
import CircularProgress from '@material-ui/core/CircularProgress';
import {useTransition, config , animated} from 'react-spring'
import {makePayment} from './remita'

const AnimatedGrid = animated(Grid)
const useStyles = makeStyles((theme) => ({
  modal: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
  paper: {
    position: 'relative',
    backgroundColor: theme.palette.background.paper,
    border: '2px solid #000',
    boxShadow: theme.shadows[5],
    padding: theme.spacing(2, 4, 3),
    width: '100ch',
    '@media (max-width: 600px)':{
      width: 'auto'
    }
  },
}));


export default function Form(props) {
  const classes = useStyles(theme);
  const [type, _setType] = useState('')
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState(null)
  const [selectedPackage, setP] = useState('')
  const [fields, setFields] = useState([]);

  const BuyButton = {
    id:2,
    xs:6,
    Field: () => {
      return (
        <Button fullWidth variant='outlined' type="submit">
          Buy
        </Button>
      )
    }
  }

  const ChoosePackage = {
    id: 1,
    xs: 12,
    Field: ({data, selectedPackage, setP }) => {
      return (
        <TextField
          fullWidth
          required
          select
          name="selectPackage"
          id="outlined-required-selectExamPackage"
          label="Select Exam Package"
          variant="outlined"
          value={selectedPackage}
          onChange={event => setP(event.target.value)}
          placeholder="Select Exam Package"
          InputLabelProps={{
            shrink: true,
          }}>
          {data && data.products.map((item) => (
            <MenuItem key={item.code} value={item.code}>
              <div style={{display:'flex',justifyContent:'space-between',width:'100%'}}>
                <div>{item.name}</div><div>{`₦${item.topup_value}`}</div>
              </div>

            </MenuItem>
          ))}
        </TextField>
        )
    }
  }


  const setType = value => {
    if (type !== value) {
      _setType(value)
      setLoading(true)
      ATApi.examInfo(value).then(res => {
        setData(res.data)
        if (fields.filter(field => field.id === 1).length === 0){
          setFields([ChoosePackage])
        }
        setLoading(false)
      }).catch(err => {
        setLoading(false)
        console.error(err)
      })
    }
  }

  useEffect(() => {
    if (selectedPackage && (fields.filter(field => field.id === 2).length === 0)) {
      const nfields = [...fields]
      nfields.push(BuyButton)
      setFields(nfields)
    }
  }, [selectedPackage, fields, BuyButton])

  const formProps = { data, selectedPackage, setP}

  const transitions = useTransition(fields, item => item.id, {
    config: config.stiff,
    // ref: transRef,
    unique: true,
    trail: 0,
    reset: true,
    from: { opacity: 0, transform: 'scale(0)', position: "absolute" },
    enter: { opacity: 1, transform: 'scale(1)', position: "relative" },
    leave: { opacity: 0, transform: 'scale(0)', position: "absolute"}
  })

  const submit = e => {
    e.preventDefault()
    const [selected] = data.products.filter(item => item.code === selectedPackage)
    const SuccessFunc = response => {
      ATApi.examPin(type, selectedPackage).then(res => {
        console.log(res.data);
        alert(res.data)
      })
    }
    const errorFunc = (error) => {
      alert(error.message)
    }
    props.close()
    makePayment(selected.topup_value, SuccessFunc, errorFunc)
  }

  return (
    <Paper className={classes.paper} elevation={3}>
      <Close close={props.close}/>
      <Typography variant='h4'>
        Buy Exam Pin
      </Typography>
      <Typography variant='caption'></Typography>
      <form validate='true' onSubmit={submit}>
        <Grid container spacing={2} justify={!loading? 'flex-start' : 'center'} style={{marginTop: theme.spacing(2)}}>
          {loading ? <CircularProgress style={{marginTop: theme.spacing(4)}}/> : <>
            <Grid item xs={12}>
              <TextField
                fullWidth
                id="outlined-select-type"
                select
                label="Select Internet Service Provider"
                value={type}
                helperText="Please select your internet service provider"
                variant="outlined"
                onChange={event => setType(event.target.value)}>
                <MenuItem value="jamb">
                  Jamb
                </MenuItem>
                <MenuItem value="waec">
                  Waec
                </MenuItem>
              </TextField>
            </Grid>

            {(fields.length > 0) && transitions.map(({ item, key, props }) => (
              <AnimatedGrid key={item.id} style={{...props}} item xs={item.xs}>
                {item.Field(formProps)}
              </AnimatedGrid>
            ))}

          </>}
        </Grid>
      </form>
    </Paper>
  )
}