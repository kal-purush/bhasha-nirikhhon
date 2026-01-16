# React_App
This project involves building a REACT and Node.js web app with PostgreSQL database. It creates 50 records with fields including name, age, phone, location, and timestamp. Features include search, pagination, and sorting by date/time.
import React, { useState } from "react";
import DataTable from "react-data-table-component";
function App() {
  const dateSortFun = (a, b) => {
    const dateA = new Date(a.created_at_date.split("-").reverse().join("-"));
    const dateB = new Date(b.created_at_date.split("-").reverse().join("-"));
    return dateA.getTime() - dateB.getTime();
  };
  const timeSortFun = (a, b) => {
    const timeA = a.created_at_time.split(",").map((item) => parseInt(item));
    const timeB = b.created_at_time.split(",").map((item) => parseInt(item));
    for (let i = 0; i < timeA.length; i++) {
      if (timeA[i] !== timeB[i]) {
        return timeA[i] - timeB[i];
      }
    }
    return 0;
  };
  const handleFilterData = (val) => {
    let filterValues = originalData.filter(
      (item) =>
        item.name.toLowerCase().includes(val.toLowerCase()) ||
        item.location.toLowerCase().includes(val.toLowerCase())
    );
    setSearchableData(filterValues);
  };
  const columns = [
    {
      name: "Sno",
      selector: (row) => row.sno,
    },
    {
      name: "Name",
      selector: (row) => row.name,
    },
    {
      name: "Age",
      selector: (row) => row.age,
    },
    {
      name: "Phone",
      selector: (row) => row.phone,
    },
    {
      name: "location",
      selector: (row) => row.location,
    },
    {
      name: "created_at_date",
      selector: (row) => row.created_at_date,
      sortable: true,
      sortFunction: dateSortFun,
    },
    {
      name: "created_at_time",
      selector: (row) => row.created_at_time,
      sortable: true,
      sortFunction: timeSortFun,
    },
  ];
  const rows = [
    {
      sno: 1,
      name: "prashanth",
      age: 23,
      phone: 9876543210,
      location: "delhi",
      created_at_date: "2-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "7hr,40min,16s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 2,
      name: "uday",
      age: 21,
      phone: 394838493,
      location: "pune",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 3,
      name: "shetty",
      age: 25,
      phone: 3948392002,
      location: "hyderabad",
      created_at_date: "24-3-2023", //`${day}-${month}-${year}`,
      created_at_time: "15hr,33min,39s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 4,
      name: "vikramadityan",
      age: 33,
      phone: 3943920284,
      location: "kolkatta",
      created_at_date: "2-11-1990", //`${day}-${month}-${year}`,
      created_at_time: "9hr,44min,22s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 5,
      name: "raj",
      age: 35,
      phone: 4839284839,
      location: "kerala",
      created_at_date: "20-9-2002", //`${day}-${month}-${year}`,
      created_at_time: "5hr,20min,26s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 6,
      name: "sai",
      age: 40,
      phone: 8493024920,
      location: "kachiguda",
      created_at_date: "3-7-1999", //`${day}-${month}-${year}`,
      created_at_time: "3hr,44min,22s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 7,
      name: "punith",
      age: 2,
      phone: 3948302833,
      location: "karnataka",
      created_at_date: "19-8-2007", //`${day}-${month}-${year}`,
      created_at_time: "24hr,30min,40s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 8,
      name: "ajay",
      age: 22,
      phone: 2939473829,
      location: "mysore",
      created_at_date: "22-12-2003", //`${day}-${month}-${year}`,
      created_at_time: "1hr,0min,59s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 9,
      name: "harsha",
      age: 19,
      phone: 384722929,
      location: "maharastra",
      created_at_date: "20-1-2024", //`${day}-${month}-${year}`,
      created_at_time: "3hr,49min,34s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 10,
      name: "harish",
      age: 21,
      phone: 374839292,
      location: "punjab",
      created_at_date: "16-5-2011", //`${day}-${month}-${year}`,
      created_at_time: "12hr,30min,26s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 11,
      name: "sushanth",
      age: 21,
      phone: 9283748392,
      location: "bangalore",
      created_at_date: "12-8-2024", //`${day}-${month}-${year}`,
      created_at_time: "7hr,40min,26s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 12,
      name: "bablu",
      age: 50,
      phone: 304939482,
      location: "pondichery",
      created_at_date: "29-6-2014", //`${day}-${month}-${year}`,
      created_at_time: "3hr,46min,16s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 13,
      name: "rambabu",
      age: 21,
      phone: 9875461258,
      location: "pune",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 14,
      name: "anil",
      age: 25,
      phone: 9458961563,
      location: "jaipur",
      created_at_date: "2-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "12hr,46min,44s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 15,
      name: "manoj",
      age: 21,
      phone: 9854785962,
      location: "bhadrahalam",
      created_at_date: "1-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "12hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 16,
      name: "tharun",
      age: 26,
      phone: 9548931569,
      location: "Delhi",
      created_at_date: "1-2-2024", //`${day}-${month}-${year}`,
      created_at_time: "11hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 17,
      name: "uday",
      age: 21,
      phone: 9851463259,
      location: "Indore",
      created_at_date: "3-1-2024", //`${day}-${month}-${year}`,
      created_at_time: "11hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 18,
      name: "roshan",
      age: 21,
      phone: 9875621458,
      location: "Jaipur",
      created_at_date: "6-8-2024", //`${day}-${month}-${year}`,
      created_at_time: "10hr,15min,45s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 19,
      name: "prashuu",
      age: 24,
      phone: 9658412563,
      location: "Bhadrahalam",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 20,
      name: "Sanjuu",
      age: 23,
      phone: 9658745823,
      location: "Bhadrahalam",
      created_at_date: "5-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 21,
      name: "ravi",
      age: 21,
      phone: 6598125463,
      location: "Delhi",
      created_at_date: "8-2-2024", //`${day}-${month}-${year}`,
      created_at_time: "23hr,46min,14s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 22,
      name: "janu",
      age: 21,
      phone: 394838493,
      location: "pune",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 23,
      name: "suraj",
      age: 21,
      phone: 394838493,
      location: "chennai",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 24,
      name: "hema",
      age: 21,
      phone: 9948384493,
      location: "Jammu",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 25,
      name: "latha",
      age: 18,
      phone: 9569854156,
      location: "Madras",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 26,
      name: "Viraj",
      age: 26,
      phone: 9815543210,
      location: "delhi",
      created_at_date: "2-3-2004", //`${day}-${month}-${year}`,
      created_at_time: "7hr,40min,16s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 27,
      name: "nikitha",
      age: 21,
      phone: 9394838493,
      location: "pune",
      created_at_date: "3-3-2014", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 28,
      name: "shahuu",
      age: 26,
      phone: 9548392002,
      location: "hyderabad",
      created_at_date: "24-3-2020", //`${day}-${month}-${year}`,
      created_at_time: "15hr,33min,39s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 29,
      name: "aditya",
      age: 35,
      phone: 9543920284,
      location: "kolkatta",
      created_at_date: "2-11-1999", //`${day}-${month}-${year}`,
      created_at_time: "9hr,44min,22s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 30,
      name: "raju",
      age: 35,
      phone: 9859284839,
      location: "kerala",
      created_at_date: "20-9-2002", //`${day}-${month}-${year}`,
      created_at_time: "5hr,29min,26s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 34,
      name: "sairaj",
      age: 40,
      phone: 8445621547,
      location: "kachiguda",
      created_at_date: "3-7-1999", //`${day}-${month}-${year}`,
      created_at_time: "3hr,48min,22s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 32,
      name: "poojitha",
      age: 23,
      phone: 9648346833,
      location: "karnataka",
      created_at_date: "19-8-2007", //`${day}-${month}-${year}`,
      created_at_time: "24hr,40min,40s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 33,
      name: "vijay",
      age: 24,
      phone: 9534573829,
      location: "mysore",
      created_at_date: "22-12-2003", //`${day}-${month}-${year}`,
      created_at_time: "1hr,0min,59s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 34,
      name: "harish",
      age: 19,
      phone: 984022929,
      location: "maharastra",
      created_at_date: "20-1-2024", //`${day}-${month}-${year}`,
      created_at_time: "3hr,49min,34s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 35,
      name: "pranith",
      age: 21,
      phone: 974799292,
      location: "punjab",
      created_at_date: "16-5-2011", //`${day}-${month}-${year}`,
      created_at_time: "12hr,30min,26s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 36,
      name: "vinay",
      age: 26,
      phone: 9683458392,
      location: "bangalore",
      created_at_date: "12-8-2024", //`${day}-${month}-${year}`,
      created_at_time: "7hr,40min,26s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 37,
      name: "ramu",
      age: 50,
      phone: 964029482,
      location: "pondichery",
      created_at_date: "29-6-2014", //`${day}-${month}-${year}`,
      created_at_time: "3hr,46min,16s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 38,
      name: "srinu",
      age: 21,
      phone: 9875791258,
      location: "pune",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 39,
      name: "mohan",
      age: 25,
      phone: 9451241563,
      location: "jaipur",
      created_at_date: "2-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "12hr,46min,44s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 40,
      name: "rajesh",
      age: 21,
      phone: 98547123962,
      location: "bhadrahalam",
      created_at_date: "1-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "12hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 41,
      name: "vinay",
      age: 26,
      phone: 9589931569,
      location: "Delhi",
      created_at_date: "1-2-2024", //`${day}-${month}-${year}`,
      created_at_time: "11hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 42,
      name: "vinod",
      age: 21,
      phone: 9824463259,
      location: "Indore",
      created_at_date: "3-1-2024", //`${day}-${month}-${year}`,
      created_at_time: "11hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 43,
      name: "vimal",
      age: 21,
      phone: 9876521458,
      location: "Jaipur",
      created_at_date: "6-8-2024", //`${day}-${month}-${year}`,
      created_at_time: "10hr,15min,45s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 44,
      name: "akhil",
      age: 24,
      phone: 9651712563,
      location: "Bhadrahalam",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 45,
      name: "manasa",
      age: 23,
      phone: 9651445823,
      location: "Bhadrahalam",
      created_at_date: "5-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 46,
      name: "srhuthi",
      age: 21,
      phone: 6598325463,
      location: "Delhi",
      created_at_date: "8-2-2024", //`${day}-${month}-${year}`,
      created_at_time: "23hr,46min,14s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 47,
      name: "rajesh",
      age: 21,
      phone: 944838493,
      location: "pune",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 48,
      name: "vivek",
      age: 21,
      phone: 924838493,
      location: "chennai",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 49,
      name: "vasanthi",
      age: 21,
      phone: 9944564493,
      location: "Jammu",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
    {
      sno: 50,
      name: "kumari",
      age: 18,
      phone: 9569154156,
      location: "Madras",
      created_at_date: "3-3-2024", //`${day}-${month}-${year}`,
      created_at_time: "13hr,26min,54s", //`${hours}hr,${minutes}min,${seconds}s`,
    },
     ];
  const [originalData, setOriginalData] = useState(rows);
  const [searchableData, setSearchableData] = useState(rows);
  return (
    <div className="container mt-5 mb-5">
      <input
        type="text"
        className="form-control ml-auto w-50"
        onChange={(e) => handleFilterData(e.target.value)}
      />
      <DataTable
        columns={columns}
        data={searchableData}
        pagination
        fixedHeader
      ></DataTable>
    </div>
  );
}

export default App;