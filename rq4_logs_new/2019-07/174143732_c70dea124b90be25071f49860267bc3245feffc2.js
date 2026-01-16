import React from "react";

export default function DataTable(props) {
  console.log(props._data);
  if (props._data != null) {
    return (
      <table className="table table-sm table-bordered text-light">
        <TableHeader title={props._title} data={props._data} />
        <TableBody data={props._data} />
      </table>
    );
  } else {
    return <div />;
  }
}
function TableHeader(props) {
  let Header_Items = Object.keys(props.data[0]).map((item, index) => (
    <th>
      <small>
        <b>{item}</b>
      </small>
    </th>
  ));
  return (
    <thead className="text-center">
      <tr className="title">
        <th colSpan={Header_Items.length}>
          <b>{props.title}</b>
        </th>
      </tr>
      <tr className="headers">{Header_Items}</tr>
    </thead>
  );
}
function TableBody(props) {
  let dataItems = props.data.map((item, index) => <tr>{rows(item, index)}</tr>);
  return <tbody>{dataItems}</tbody>;
}
function rows(item, index) {
  let data_item = Object.values(item).map((prop, index_) => (
    <td>
      <small>{prop}</small>
    </td>
  ));
  return data_item;
}