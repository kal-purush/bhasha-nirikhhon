import _ from "lodash";
import React, { Component } from "react";
import { employeeUpdate, employeeSave, employeeDelete } from "../services/actions"
import { connect } from "react-redux"
import { Card, CardItem, Button, ConfirmAction } from "./common"
import { EmployeeForm } from "./EmployeeForm"
import Communications from "react-native-communications"

class EmployeeEdit extends Component {

    state = { showModal: false }

    componentWillMount() {
        const { employee } = this.props;
        _.each(employee, (value, prop) => {
            this.props.employeeUpdate({ prop, value })
        })
    }

    onButtonPress() {
        const { name, phone, shift } = this.props;
        this.props.employeeSave({ name, phone, shift, uid: this.props.employee.uid })
    }

    onTextPress() {
        const { phone, shift } = this.props;
        Communications.text(phone, `Your upcoming shift is on ${shift}`)
    }

    onDecline() {
        this.setState({ showModal: !this.state.showModal })
    }

    onAccept() {
        const { uid } = this.props.employee
        this.props.employeeDelete({ uid });
    }

    render() {
        return (
            <Card>

                <EmployeeForm />

                <CardItem>
                    <Button handlePress={this.onButtonPress.bind(this)}>
                        Save Changes
                    </Button>
                </CardItem>

                <CardItem>
                    <Button handlePress={this.onTextPress.bind(this)}>
                        Send Text
                    </Button>
                </CardItem>

                <CardItem>
                    <Button onPress={() => this.setState({ showModal: !this.state.showModal })}>
                        Fire Employee
                    </Button>
                </CardItem>

                <ConfirmAction
                    visible={this.state.showModal}
                    onAccept={this.onAccept.bind(this)}
                    onDecline={this.onDecline.bind(this)}
                >
                    Are you sure you want to fire this employee?
                </ConfirmAction>
            </Card>
        )
    }
}


const mapStateToProps = (state) => {
    const { name, phone, shift } = state.EmployeeForm;
    return { name, phone, shift }
}

export default connect(mapStateToProps,
    {
        employeeUpdate,
        employeeSave,
        employeeDelete
    }
)(EmployeeEdit);