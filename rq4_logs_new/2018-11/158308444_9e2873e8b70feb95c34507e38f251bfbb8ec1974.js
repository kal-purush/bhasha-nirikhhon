import React, { Component } from "react";
import { Row, Col, Button, Card, CardBody, Input, Container } from "mdbreact";
import axios from "axios";
import { edit, fullEdit } from "../components/EditFunction";
import Select from "react-select";
import Dialog from "@material-ui/core/Dialog";
import DialogActions from "@material-ui/core/DialogActions";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogTitle from "@material-ui/core/DialogTitle";

class ManageAccount extends Component {
  constructor(props) {
    super(props);
    this.state = {
      emailD: "",
      exist: false,
      id_person: "",
      person_data: "",
      open: false,
      firstName: "",
      secondName: "",
      lastName1: "",
      lastName2: "",
      idNumber: "",
      birthDate: "",
      phone: "",
      address: "",
      accounts: [{}],

    };
    this.handleInputChange = this.handleInputChange.bind(this);
    this.handleChange = this.handleChange.bind(this);
    this.verifyAccount = this.verifyAccount.bind(this);
    this.disableAccount = this.disableAccount.bind(this);
    this.accountSelect = this.accountSelect.bind(this);
    // this.loadIntoSelect = this.loadIntoSelect.bind(this);
    // this.accountSelector = this.accountSelector.bind(this);
  }


  componentDidMount() {
    document.getElementById("Formulario").style.display = "none";
    axios.get("http://localhost:8080/ShowAccounts").then(response => {
      this.state.accounts = response.data;
      this.setState({
        accounts: response.data
      });
    });
  }

  async verifyAccount() {
    this.setState({ person_data: "" });
    try {
      const response = await axios.get(
        `users/verifyAccount/${this.state.emailD}`
      );
      if (response) {
        const token2 = {
          id_person: response.data.id_person,
        };
        edit(token2).then(res => {
          if (res) {
            this.setState({
              firstName: res[0].FirstNameJs,
              secondName: res[0].SecondNameJs,
              lastName1: res[0].lastName1Js,
              lastName2: res[0].lastName2Js,
              birthDate: res[0].birthDateJs,
              idNumber: res[0].identificationJs,
              address: res[0].addressJs,
              phone: res[0].phoneJs
            });
          }
        });
      }
      this.setState({ id_person: response.data.id_person });
      try {
        const response2 = await axios.get(
          `users/getPersonData/${this.state.id_person}`
        );
        this.setState({
          person_data:
            response2.data.first_name + " " + response2.data.last_name_1
        });
      } catch (err) {
        console.error(err);
      }
    } catch (err) {
      console.error(err);
    }
  }

  async disableAccount() {
      try {
        const response = await axios
          .put(`users/disableAccount/${this.state.emailD}`)
          .then(response => {
            if (response) {
              //this.props.history.push(`/profile`);
            }
          });
      } catch (err) {
        console.error(err);
      }  
  }

  handleChange(event) {
    const { name, value } = event.target;
    this.setState({
      [name]: value
    });
  }
  handleClickOpen = () => {
    this.setState({ open: true });
  };


  handleClose = () => {
    this.setState({ open: false });
  };


  handleSubmit = evt => {
    event.preventDefault();
    const valuesToEdit = {
      firstName: this.state.firstName,
      secondName: this.state.secondName,
      lastName1: this.state.lastName1,
      lastName2: this.state.lastName2,
      idNumber: this.state.idNumber,
      birthDate: this.state.birthDate,
      phone: this.state.phone,
      address: this.state.address,
      id_person: this.state.id_person,
    };

    fullEdit(valuesToEdit).then(res => {
      if (res) {
        this.props.history.push(`/`);
      }
    });
  };

  handleInputChange(event) {
    const { name, value } = event.target;
    this.setState({
      [name]: value
    });
  }

  async accountSelect(event) {
    this.state.emailD = event.value;
    this.setState({ emailD: event.value });
    this.setState({ person_data: "" });
    try {
      const response = await axios.get(
        `users/verifyAllAccount/${this.state.emailD}`
      );
      if (response) {
        this.setState({ id_person: response.data.id_person });
        const token2 = {
          id_person: response.data.id_person,
        };
        edit(token2).then(res => {
          if (res) {
            this.setState({
              firstName: res[0].FirstNameJs,
              secondName: res[0].SecondNameJs,
              lastName1: res[0].lastName1Js,
              lastName2: res[0].lastName2Js,
              birthDate: res[0].birthDateJs,
              idNumber: res[0].identificationJs,
              address: res[0].addressJs,
              phone: res[0].phoneJs
            });
          }
        });
      }
    } catch (err) {
      console.error(err);
    }
    document.getElementById("Formulario").style.display = "block";

  }

  render() {
    return (
      <div className="container">
        <Row>
          <Col className="mx-auto mt-5">
            <Card>
              <CardBody>
                <p className="h5 text-center mb-4">Administrar Cuentas</p>

                <label>Elija la cuenta: </label>
                <Select
                  onChange={this.accountSelect}
                  options={this.state.accounts.map(function (json) {
                    return {
                      label: json.username + " /// " + json.email,
                      value: json.email
                    };
                  })}
                />


                {/*                 <div className="col">
                  <Input
                    label="Email"
                    name="emailD"
                    maxLength="30"
                    value={this.state.emailD}
                    onChange={this.handleChange}
                    hint="ejemplo@mail.com"
                    type="email"
                    required
                  />
                  <small id="emailHelp" className="form-text text-muted">
                    Dijite el email de la cuenta.
                  </small>
                </div> */}

                <div>
                  {/*                   <Button
                    className="btn btn-outline-deep-orange"
                    onClick={this.verifyAccount}
                  >
                    Verificar
                  </Button> */}
                  <div id="Formulario">
                    <Container className="mt-5">
                      <Row className="mt-6">
                        <Col md="8" className="mx-auto">
                          <Card>
                            <CardBody>
                              <form
                                className="needs-validation"
                                onSubmit={this.handleSubmit}
                                noValidate
                              >
                                <label className="cyan-text">Datos personales:</label>
                                <div className="row">
                                  <div className="col">
                                    <Input
                                      label="Primer Nombre"
                                      name="firstName"
                                      maxLength="25"
                                      value={this.state.firstName}
                                      onChange={this.handleInputChange}
                                      type="text"
                                      required
                                    />
                                  </div>
                                  <div className="col">
                                    <Input
                                      label="Segundo Apellido"
                                      name="secondName"
                                      maxLength="25"
                                      value={this.state.secondName}
                                      onChange={this.handleInputChange}
                                      type="text"
                                      required
                                    />
                                  </div>
                                  <div className="col">
                                    <Input
                                      label="Primer Apellido"
                                      name="lastName1"
                                      maxLength="25"
                                      value={this.state.lastName1}
                                      onChange={this.handleInputChange}
                                      type="text"
                                      required
                                    />
                                  </div>
                                  <div className="col">
                                    <Input
                                      label="Segundo Apellido"
                                      name="lastName2"
                                      maxLength="25"
                                      value={this.state.lastName2}
                                      onChange={this.handleInputChange}
                                      type="text"
                                      required
                                    />
                                  </div>
                                </div>
                                <div className="row">
                                  <div className="col">
                                    <Input
                                      label="Cédula"
                                      name="idNumber"
                                      maxLength="25"
                                      value={this.state.idNumber}
                                      onChange={this.inputNumberValidator}
                                      type="text"
                                      required
                                    />
                                  </div>
                                  <div className="col-6">
                                    <Input
                                      label="Fecha Nacimiento"
                                      name="birthDate"
                                      value={this.state.birthDate}
                                      onChange={this.handleInputChange}
                                      type="date"
                                      hint="00/00/0000"
                                      required
                                    />
                                  </div>
                                  <div className="col">
                                    <Input
                                      label="Teléfono"
                                      name="phone"
                                      maxLength="8"
                                      value={this.state.phone}
                                      onChange={this.inputNumberValidator}
                                      type="text"
                                      required
                                    />
                                  </div>
                                </div>
                                <div className="row">
                                  <div className="col">
                                    <Input
                                      label="Dirección"
                                      name="address"
                                      value={this.state.address}
                                      onChange={this.handleInputChange}
                                      type="textarea"
                                      hint="Dirección exacta"
                                      rows="1"
                                      maxLength="250"
                                      required
                                    />
                                    <Input
                                      name="email"
                                      value={this.state.email}
                                      type="hidden"
                                    />
                                  </div>
                                </div>
                                <div className="text-center py-4 mt-3">
                                  <button
                                    className="btn btn-outline-deep-orange"
                                    type="submit" to="/profile">
                                    Editar
                            </button>
                                </div>
                              </form>

                              <Button
                              
                                className="btn btn-outline-deep-orange"
                                onClick={this.handleClickOpen}
                              >
                                Deshabilitar
                  </Button>
                              <Dialog
                                open={this.state.open}
                                onClose={this.handleClose}
                                aria-labelledby="alert-dialog-title"
                                aria-describedby="alert-dialog-description"
                              >
                                <DialogTitle id="alert-dialog-title">
                                  {"Desactivar Cuenta"}
                                </DialogTitle>
                                <DialogContent>
                                  <DialogContentText id="alert-dialog-description">
                                    Está seguro que desea deshabilitar la cuenta del
                        usuario: "{this.state.emailD} " ?
                      </DialogContentText>
                                </DialogContent>
                                <DialogActions>
                                  <Button onClick={this.handleClose} color="primary">
                                    Cancelar
                      </Button>
                                  <Button
                                    onClick={() => {
                                      this.disableAccount();
                                      this.handleClose();
                                    }}
                                    color="primary"
                                    autoFocus
                                  >
                                    Aceptar
                      </Button>
                                </DialogActions>
                              </Dialog>
                            </CardBody>
                          </Card>
                        </Col>
                      </Row>
                    </Container>
                  </div>





                </div>
              </CardBody>
            </Card>
          </Col>
        </Row>
      </div>
    );
  }
}
export default ManageAccount;