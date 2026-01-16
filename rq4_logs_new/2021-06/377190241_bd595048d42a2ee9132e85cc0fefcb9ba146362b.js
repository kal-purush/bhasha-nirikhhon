import { Component } from "react";
import FormControl from "react-bootstrap/FormControl";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import Table from "react-bootstrap/Table";
import LinkButton from "../../../FuncComponents/LinkButton";
import { AdminService } from "../../../Services/AdminService";
import { Form } from "react-bootstrap";
import ConfirmDepartmentModal from "../../../FuncComponents/ConfirmDepartmentModal";

class AdminDepartmentsPanel extends Component {
  constructor(props) {
    super();
    this.state = {
      departmentsList: null,
      isActive: null,
      search:'',
      skip: 0,
      limit: 50,
    };
  }

  componentWillMount() {
    const { isActive, skip, limit, search } = this.state;
    AdminService.getPanel("departments", isActive, skip, limit, search).then(
      (result) => {
        this.setState({
          departmentsList: result,
        });
      }
    );
  }

  componentDidUpdate() {
    // const { isActive, skip, limit} = this.state;
    // AdminService.getPanel("organizations", isActive, skip, limit).then((result) => {
    //   this.setState({
    //     departmentsList: result,
    //   });
    // });
  }

  radioSubmit(e) {
    e.preventDefault();
    const { isActive, skip, limit, search } = this.state;
    AdminService.getPanel("departments", isActive, skip, limit, search).then(
      (result) => {
        this.setState({
          departmentsList: result,
        });
      }
    );
  }

  onValueChange(event) {
    this.setState({
      isActive: event.target.value,
    });
  }

  confirmHandler(id) {
    const {isActive, skip, limit, search } = this.state;
    AdminService.confirmDepartment(id).then((s) => {
      AdminService.getPanel("departments", isActive, skip, limit, search ).then(
        (result) => {
          this.setState({
            departmentsList: result,
          });
        }
      );
    });
  }
  handleChange(e) {
    const { name, value } = e.target;
    this.setState({ [name]: value });
  }
  handleSearch(e) {
    e.preventDefault();
    const { isActive, skip, limit, search } = this.state;
    AdminService.getPanel("departments", isActive, skip, limit, search).then(
      (result) => {
        this.setState({
          departmentsList: result,
        });
      }
    );
  }
  render() {
    const { departmentsList, search } = this.state;
    return (
      <>
        <h3>Управління відділеннями</h3>
        <InputGroup className="mb-3">
          <FormControl
            className="border border-info"
            aria-describedby="basic-addon1"
            placeholder="Знайти відділення за його назвою"
            name="search"
            value={search}
            onChange={(e) => this.handleChange(e)}
          />
          <InputGroup.Append>
            <Button variant="outline-info" onClick={(e) => this.handleSearch(e)}>Шукати</Button>
          </InputGroup.Append>
        </InputGroup>

        <Form onSubmit={(e) => this.radioSubmit(e)}>
          {["radio"].map((type) => (
            <div key={`inline-${type}`} className="mb-3">
              <Form.Check
                inline
                label="Всі"
                name="selectgroup"
                defaultChecked
                type={type}
                id={`radioAll`}
                value="null"
                onChange={(e) => this.onValueChange(e)}
              />
              <Form.Check
                inline
                label="Підтверджені"
                name="selectgroup"
                type={type}
                id={`radioConfirmed`}
                value="true"
                onChange={(e) => this.onValueChange(e)}
              />
              <Form.Check
                inline
                label="Не підтверджені"
                type={type}
                name="selectgroup"
                id={`radioNonConfirmed`}
                value="false"
                onChange={(e) => this.onValueChange(e)}
              />
              <Button type="submit">Показати</Button>
            </div>
          ))}
        </Form>

        <Table bordered hover>
          <thead>
            <tr className="bg-info text-light">
              <th>Назва відділення</th>
              <th>Назва організації</th>
              <th>ЄДРПОУ</th>
              <th>Повна адреса</th>
              <th>Дата реєстрації</th>
              <th>Статус</th>
            </tr>
          </thead>
          <tbody>
            {departmentsList && (
              <>
                {departmentsList.data.map((item) => (
                  <>
                    {item.isActive == true ? (
                      <tr className="table" key={item.id}>
                        <td>{item.name}</td>
                        <td>{item.organization.name}</td>
                        <td>{item.organization.edrpou}</td>
                        <td>{item.addressText}</td>
                        <td>{item.createdAt}</td>
                        <td>Підтверджено</td>
                      </tr>
                    ) : item.isActive == false ? (
                      <tr className="table-warning" key={item.id}>
                        <td>{item.name}</td>
                        <td>{item.organization.name}</td>
                        <td>{item.organization.edrpou}</td>
                        <td>{item.addressText}</td>
                        <td>{item.createdAt}</td>
                        <td>
                          Не підтверджено{" "}
                          <ConfirmDepartmentModal
                            id={item.id}
                            name={item.name}
                            addressText={item.addressText}
                            confirmHandler={() => this.confirmHandler(item.id)}
                          ></ConfirmDepartmentModal>
                        </td>
                      </tr>
                    ) : (
                      <></>
                    )}
                  </>
                ))}
              </>
            )}
          </tbody>
        </Table>
      </>
    );
  }
}

export default AdminDepartmentsPanel;