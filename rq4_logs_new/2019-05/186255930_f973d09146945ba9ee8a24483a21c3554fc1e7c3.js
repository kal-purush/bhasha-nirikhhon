import React from "react";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";

export default class PortDate extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            dateValue: new Date()
        };
        this.handleChange = this.handleChange.bind(this);
    }

    componentDidMount() {
        this.setState({ isBrowserLoaded: true })
    }

    handleChange(date) {
        // debugger;

        // const formattedDate = date.format();
        this.setState({
            dateValue: date
        });
    }

    render() {
        const { isBrowserLoaded } = this.state;

        return (
            <React.Fragment>
                {isBrowserLoaded && <DatePicker
                    selected={this.state.dateValue}
                    onChange={this.handleChange}
                />
                }
            </React.Fragment>
        );
    }
}