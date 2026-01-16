import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux'
import { createMuiTheme, MuiThemeProvider } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import { color1 } from '../constants/colors'
import ConsumptionBarChart from '../leads/ConsumptionBarChart'
import AccessTimeIcon from '@material-ui/icons/AccessTime';
import FlashOnIcon from '@material-ui/icons/FlashOn';

const theme = createMuiTheme({
    typography: {
        useNextVariants: true,
    },
    palette: {
        secondary: {
            main: color1,
        },
    },
});

export class ConsumptionComponent extends Component {
    state = {
        hours: 0,
        minutes: 0,
        seconds: 0,
    }
    componentDidMount() {

        var sec = Math.floor((Date.now() - this.props.startTime) / 1000)
        var seconds = Math.floor(sec % 60)
        var minutes = Math.floor((sec % 3600) / 60)
        var hours = Math.floor(sec / 3600)
        this.setState({ hours, minutes, seconds })
        this.myInterval = setInterval(() => {
            const { seconds, minutes } = this.state

            if (seconds === 59) {
                this.setState(({ minutes }) => ({
                    minutes: minutes + 1,
                    seconds: 0
                }))
            } else {
                this.setState(({ seconds }) => ({
                    seconds: seconds + 1
                }))
            }

            if (minutes === 59) {
                this.setState(({ hours }) => ({
                    hours: hours + 1,
                    minutes: 0
                }))
            }
        }, 1000)
    }
    componentWillUnmount() {
        clearInterval(this.myInterval)
    }

    render() {
        const { hours, minutes, seconds } = this.state
        return (
            <MuiThemeProvider theme={theme}>
                <div>
                    <Typography gutterBottom variant="h5" component="h2" color="secondary">
                        <AccessTimeIcon />Elapsed Time
                    </Typography>
                    <div className="row">
                        <div className="col-12">
                            <h1 className="text-center">{`${hours < 10 ? '0' : ''}${hours}:${minutes < 10 ? '0' : ''}${minutes}:${seconds < 10 ? '0' : ''}${seconds}`}</h1>
                        </div>
                    </div>
                    <Typography gutterBottom variant="h5" component="h2" color="secondary">
                        <FlashOnIcon />Total Energy Used
                    </Typography>
                    <Typography align="center" variant="h3" color="primary">
                        234kWH
                    </Typography>
                    <ConsumptionBarChart />
                </div>
            </MuiThemeProvider>
        );
    }
}

ConsumptionComponent.propTypes = {
    startTime: PropTypes.string.isRequired,
};
const mapStateToProps = state => ({
    startTime: state.ongoing.records[0].start
})


export default connect(mapStateToProps, {})(ConsumptionComponent)