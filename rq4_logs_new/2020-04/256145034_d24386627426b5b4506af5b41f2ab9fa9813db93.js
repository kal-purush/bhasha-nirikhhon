import React, { Component, useState } from 'react';
import { Container, Table } from 'reactstrap';
import styled from 'styled-components';

const getMonthName = (date) => {
  const monthNames = ["January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
  ];
  return monthNames[date.getMonth()];
}


const getFirstDayOfMonth = (d) => new Date(d.getFullYear(),d.getMonth(),1); 
// const getDaysInMonth = (month,year) => {
//   return new Date(year, month, 0).getDate();
//   // Months are 0 based and 0 is the last day of the previous month.
// };

const addDays = (date, days) => {
  const milliseconds = date.getTime();
  const newMilliseconds = milliseconds + 1000 * 60 * 60 * 24 * days
  return new Date(newMilliseconds);
}

const Cell = styled.td`
  height:2em;
  width:2em;
  margin:0.25em;
  cursor:pointer;
  border-radius:20px;

  &:hover {
    background: #555;
    color: #eee;
  }
`

const StyledTable = styled(Table)`
  border-radius:20px;
  border-top:unset;
  padding:1em;
`

const DayCell = (props) => {
  const d = new Date(props.milliseconds);
  const [date] = useState(d);
  const mutedText = props.isCurrentMonth ? '': 'text-secondary';
  const clickHandler = () => {
    alert(date);
  }
  return (
    <Cell className={mutedText} onClick={clickHandler}>
      {date.getDate()}
    </Cell>
  )
}

export default class Calendar extends Component {
  constructor(props){
    super(props);
    this.state = {
      user: props.user,
      currentDate: props.date,
      selectedMonth: getFirstDayOfMonth(props.date),
    }
  }
  changeSelectedMonth = (n) => {
    let month = this.state.selectedMonth.getMonth();
    let year = this.state.selectedMonth.getFullYear();
    month += n;
    if (month > 11) {
      year++;
      month = 0;
    } else if (month < 0) {
      year--;
      month = 11;
    }
    const newDate = new Date(year,month,1);
    this.setState({selectedMonth:newDate});
  }
  createRows = () => {    
    const { selectedMonth } = this.state;
    const rows = [[]];
    let rowIndex = 0;
    let d = selectedMonth;
    while (d.getDay() !== 0){
      d = addDays(d, -1);
    }
    let isFinished = false;
    while (!isFinished) {      
      const el = <DayCell 
        milliseconds={d.getTime()} 
        key={d.getTime()}
        isCurrentMonth={d.getMonth() === selectedMonth.getMonth()}
        />;
      rows[rowIndex].push(el);
      d = addDays(d, 1);
      if (rows[rowIndex].length === 7) {
        if (d.getFullYear() === selectedMonth.getFullYear()){
          isFinished = (d.getMonth() >  selectedMonth.getMonth());
        } else {
          isFinished = true;
        }
        
        rows.push([]);
        rowIndex++;
      }
    }

    const rowElements = rows.map((row, i) => {
      return (
      <tr key={`tr${i}`}>
        {row}
      </tr>
      )
    })
    
    return (
      <tbody>
        {rowElements}
      </tbody>
      
    )
  }
  
  render() {

    return (
      <Container>
        <StyledTable dark borderless className="text-center">
          <thead>
              <tr>
                <Cell colSpan="2" onClick={() => this.changeSelectedMonth(-1)}> &lt; </Cell>
                <td colSpan="3" >{`${getMonthName(this.state.selectedMonth)} ${this.state.selectedMonth.getFullYear()}`}</td>
                <Cell colSpan="2" onClick={() => this.changeSelectedMonth(1)}> > </Cell>
              </tr>
              <tr>
                <th>Sunday</th>
                <th>Monday</th>
                <th>Tuesday</th>
                <th>Wednesday</th>
                <th>Thursday</th>
                <th>Friday</th>
                <th>Saturday</th>              
              </tr>
            
          </thead>
          {this.createRows()}
        </StyledTable>
      </Container>
      
    )
  }
}