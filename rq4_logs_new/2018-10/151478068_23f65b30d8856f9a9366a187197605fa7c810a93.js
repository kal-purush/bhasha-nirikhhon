import React, { Component } from 'react';
import { AutoComplete, Col, Icon, Input, InputNumber, Row } from 'antd';
import PropTypes from 'prop-types';

const Option = AutoComplete.Option;

class BasicInfo extends Component {
  constructor(props) {
    super(props);
    this.state = {}
  }
  render() {
    return (
      <div className="basic-info">
        <Row gutter={16}>
          <Col xs={24} md={12}>
            <AutoComplete 
              allowClear={true}
              onChange={this.propshandleChange}
              onSearch={this.props.handleSearch}
              onSelect={this.props.handleSelect}
              optionLabelProp="text"
              placeholder="E-mail"
            >
              <Option
                key="Héctor Mendoza"
                text="hector@benandfrank"
              >
                <small>Héctor Mendoza</small><br/>
                hector@benandfrank
              </Option>
            </AutoComplete>
            <Icon
              name="open"
              onClick={this.props.handleModal}
              theme="outlined"
              type="user-add"
            />
          </Col>
          <Col xs={24} md={12}>
            <Input
              disabled
              placeholder="Nombre"
              value={this.props.selectedName}
            />
          </Col>
        </Row>
        <Row gutter={16}>
          <Col xs={24} md={12}>
            <InputNumber
              max={99}
              min={1}
              onChange={this.props.handleChangeAge}
              placeholder="Edad"
              value={this.props.age}
            />
          </Col>
          <Col xs={24} md={12}>
            <Input
              onChange={this.props.handleChangePhone}
              placeholder="Celular"
              value={this.props.phone}
            />
          </Col>
        </Row>
      </div>
    );
  }
}

BasicInfo.propTypes = {
  handleChange: PropTypes.func.isRequired,
  handleSearch: PropTypes.func.isRequired,
  handleSelect: PropTypes.func.isRequired,
  handleModal: PropTypes.func.isRequired,
  selectedName: PropTypes.string.isRequired,
  handleChangeAge: PropTypes.func.isRequired,
  age: PropTypes.number,
  handleChangePhone: PropTypes.func.isRequired,
  phone: PropTypes.string,
}
export default BasicInfo;