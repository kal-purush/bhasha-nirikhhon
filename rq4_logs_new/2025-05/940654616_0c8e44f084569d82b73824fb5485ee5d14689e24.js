import {
  Button,
  Chip,
  CircularProgress,
  IconButton,
  Paper,
  Popover,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Typography,
} from "@mui/material";
import ThumbUpOutlinedIcon from "@mui/icons-material/ThumbUpOutlined";
import ThumbDownOutlinedIcon from "@mui/icons-material/ThumbDownOutlined";
import ErrorOutlineOutlinedIcon from "@mui/icons-material/ErrorOutlineOutlined";
import VerifiedOutlinedIcon from "@mui/icons-material/VerifiedOutlined";
import KeyboardBackspaceIcon from "@mui/icons-material/KeyboardBackspace";
import dummyP2I from "../../../assets/img/dummyP2I.png";

import React, { useContext, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { AuthContext } from "../../../globals/AuthContext";

import styles from "./TelemetryMetrices.module.css";
import { TelemetryMetricesData as temp } from "../../../HardCodeData";
import listIcon from "../../../assets/genaiIcons/list_alt.svg"
import deselectIcon from "../../../assets/genaiIcons/deselect.svg"

import DropdownFilter from "./DropdownFilter";
import AggregatedScore from "./AggregatedScore/AggregatedScore";
import AugmentationInsights from "./AugmentationInsights/AugmentationInsights";
import EntityBasedInsights from "./EntityBasedInsights/EntityBasedInsights";
import ScoreDistribution from "./ScoreDistribution/ScoreDistribution";
const sampleFilterData = {
  "Flight Booking & Fare Deals": ["Fare Classes & Restrictions", "Multi‑City","Non‑Refundable Tickets","Price Alerts & Watchlists"],
  "Changes & Cancellations": ["Rebooking", "Cancellation Fees & Waivers","Delay Compensation"],
  "Check‑In & Boarding Assistance": ["Online & Airport Check‑In", "Group Boarding", "Security & Document Checks"],
  "Baggage Allowances & Claims":["Carry‑On vs. Checked Allowances","Excess Baggage Fees & Waivers","Gate‑Checked Items"],
  "Travel Documentation & Visas":["Passport Validity & Blank Pages","Health & Entry Formalities","Transit / Layover Regulations"],
  "Travel Insurance & Refunds":["Coverage Types","Refund & Payout Timelines","Policy Exclusions & Add‑Ons"]
};
const redList = [
  "controversiality",
  "criminality",
  "harmfullness",
  "insensitivity",
  "maliciousness",
  "misogyny",
  "pii_detction",
  "stereotype",
];

const TelemetryMetrices = (props) => {
  const [page, setPage] = useState(0);
  const [rowPage, setRowPage] = useState(5);

  const navigate = useNavigate();
  const ctx = useContext(AuthContext);

  const handleChangePage = (event, newpage) => {
    setPage(newpage);
  };

  const handleChangeRowsPerPage = (event) => {
    setRowPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const [anchorEl, setAnchorEl] = useState(null);
  const [hoverData, setHoverData] = useState({});

  const handlePopoverOpen = (event, data) => {
    setAnchorEl(event.currentTarget);
    setHoverData(data);
  };

  const handlePopoverClose = () => {
    setAnchorEl(null);
  };

  //filter
  const [selectedTopic, setSelectedTopic] = useState("");
  const [selectedSubTopics, setSelectedSubTopics] = useState([]);
  const [activeTab, setActiveTab] = useState('table')
  const handleTopicChange = (topic) => {
    setSelectedTopic(topic);
    setSelectedSubTopics([]); // Clear subtopics when topic changes
  };
  const handleSubTopicChange = (value, checked) => {
    setSelectedSubTopics((prev) =>
      checked ? [...prev, value] : prev.filter((v) => v !== value)
    );
  }

  const handleApplyFilter =() =>{
    console.log(selectedSubTopics+"-------"+selectedTopic)
  }

  //filter end



  const open = Boolean(anchorEl);
  let content = (
    <>
      <TableBody>
        {props.evaluationData.evaluation_records
          .slice(page * rowPage, page * rowPage + rowPage)
          .map((row, index) => {
            const uniqueRowNumber = page * rowPage + index + 1;
            return (
              <TableRow
                key={index}
                style={
                  index % 2
                    ? { background: "#F6F6F6" }
                    : { background: "white" }
                }
              >
                <TableCell
                  className={`${styles.stickyCol} ${styles.firstCol}`}
                  style={
                    index % 2
                      ? { background: "#F6F6F6" }
                      : { background: "white" }
                  }
                >
                  <Link
                    to={"details/" + row._id}
                    state={{ num: uniqueRowNumber }}
                  >
                    {uniqueRowNumber}
                  </Link>
                </TableCell>
                {props.evaluationData &&
                  !(props.evaluationData.evaluation.constructor === Object) && (
                    <>
                      <TableCell
                        className={`${styles.stickyCol} ${styles.stickyCol2}`}
                        style={
                          index % 2
                            ? { background: "#F6F6F6" }
                            : { background: "white" }
                        }
                      >
                        {row.question.length > 50 ? (
                          <Typography
                            aria-owns={open ? "mouse-over-popover" : undefined}
                            aria-haspopup="true"
                            onMouseEnter={(ev) =>
                              handlePopoverOpen(ev, row.question)
                            }
                            onMouseLeave={handlePopoverClose}
                          >
                            {row.question.substr(0, 50) + "..."}
                          </Typography>
                        ) : (
                          row.question
                        )}
                      </TableCell>
                      {ctx.projectSubType === "P2I" ? (
                        <TableCell>
                          <img
                            src={row.answer}
                            style={{ width: "5rem", height: "5rem" }}
                          />
                        </TableCell>
                      ) : (
                        <TableCell
                          className={`${styles.stickyCol} ${styles.stickyCol3}`}
                          style={
                            index % 2
                              ? { background: "#F6F6F6" }
                              : { background: "white" }
                          }
                        >
                          {row.answer.length > 50 ? (
                            <Typography
                              aria-owns={
                                open ? "mouse-over-popover" : undefined
                              }
                              aria-haspopup="true"
                              onMouseEnter={(ev) =>
                                handlePopoverOpen(ev, row.answer)
                              }
                              onMouseLeave={handlePopoverClose}
                            >
                              {row.answer.substr(0, 50) + "..."}
                            </Typography>
                          ) : (
                            row.answer
                          )}
                        </TableCell>
                      )}
                    </>
                  )}

                {props.evaluationData &&
                  props.evaluationData.evaluation.constructor === Object && (
                    <>
                      <TableCell
                        className={`${styles.stickyCol} ${styles.stickyCol2}`}
                        style={
                          index % 2
                            ? { background: "#F6F6F6" }
                            : { background: "white" }
                        }
                      >
                        {row.JD_Name.length > 50 ? (
                          <Typography
                            aria-owns={open ? "mouse-over-popover" : undefined}
                            aria-haspopup="true"
                            onMouseEnter={(ev) =>
                              handlePopoverOpen(ev, row.JD_Name)
                            }
                            onMouseLeave={handlePopoverClose}
                          >
                            {row.JD_Name.substr(0, 50) + "..."}
                          </Typography>
                        ) : (
                          row.JD_Name
                        )}
                      </TableCell>
                      <TableCell
                        className={`${styles.stickyCol} ${styles.stickyCol3}`}
                        style={
                          index % 2
                            ? { background: "#F6F6F6" }
                            : { background: "white" }
                        }
                      >
                        {row.Question.length > 50 ? (
                          <Typography
                            aria-owns={open ? "mouse-over-popover" : undefined}
                            aria-haspopup="true"
                            onMouseEnter={(ev) =>
                              handlePopoverOpen(ev, row.Question)
                            }
                            onMouseLeave={handlePopoverClose}
                          >
                            {row.Question.substr(0, 50) + "..."}
                          </Typography>
                        ) : (
                          row.Question
                        )}
                      </TableCell>

                      <TableCell
                        className={`${styles.stickyCol} ${styles.stickyCol4}`}
                        style={
                          index % 2
                            ? { background: "#F6F6F6" }
                            : { background: "white" }
                        }
                      >
                        {Object.keys(row).includes("JD-context") ? (
                          row["JD-context"].length > 50 ? (
                            <Typography
                              aria-owns={
                                open ? "mouse-over-popover" : undefined
                              }
                              aria-haspopup="true"
                              onMouseEnter={(ev) =>
                                handlePopoverOpen(ev, row["JD-context"])
                              }
                              onMouseLeave={handlePopoverClose}
                            >
                              {row["JD-context"].substr(0, 50) + "..."}
                            </Typography>
                          ) : (
                            row["JD-context"]
                          )
                        ) : row["JD_context"].length > 50 ? (
                          <Typography
                            aria-owns={open ? "mouse-over-popover" : undefined}
                            aria-haspopup="true"
                            onMouseEnter={(ev) =>
                              handlePopoverOpen(ev, row["JD_context"])
                            }
                            onMouseLeave={handlePopoverClose}
                          >
                            {row["JD_context"].substr(0, 50) + "..."}
                          </Typography>
                        ) : (
                          row["JD_context"]
                        )}
                      </TableCell>
                    </>
                  )}

                {props.evaluationData.evaluation_records.length > 0 &&
                  row.metrics.map(
                    (item) =>
                      !(["latency", "totalcost"].indexOf(item.name) > -1) && (
                        <TableCell style={{ position: "relative" }}>
                          {
                            typeof item.value === "string" &&
                              item.value === "NA" ? (
                              "NA"
                            ) : (
                              <Chip
                                style={{
                                  backgroundColor: (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "#EAFFFE"
                                    : "#FFEDF3",
                                }}
                                icon={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  ) ? (
                                    <VerifiedOutlinedIcon />
                                  ) : (
                                    <ErrorOutlineOutlinedIcon />
                                  )
                                }
                                label={
                                  typeof item.value === "string"
                                    ? item.value
                                    : item.value.toFixed(2)
                                }
                                variant="outlined"
                                color={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "success"
                                    : "error"
                                }
                                size="small"
                              />
                            )
                            // :
                            //   (
                            // <Chip
                            //   style={{
                            //     backgroundColor:
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? "#EAFFFE" : "#FFEDF3",
                            //   }}
                            //   icon={
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? (
                            //       <VerifiedOutlinedIcon />
                            //     ) : (
                            //       <ErrorOutlineOutlinedIcon />
                            //     )
                            //   }
                            //   label={typeof(item.value)==="string" ? item.value : item.value.toFixed(2)}
                            //   variant="outlined"
                            //   color={( item.hasOwnProperty('status') ? item.status : true ) ? "success" : "error"}
                            //   size="small"
                            // />
                            // )
                          }
                        </TableCell>
                      )
                  )}

                {props.evaluationData.evaluation_records.length > 0 &&
                  !(row.custom_metrics.constructor === Object) &&
                  row.custom_metrics.map(
                    (item) =>
                      !(["latency", "totalcost"].indexOf(item.name) > -1) && (
                        <TableCell style={{ position: "relative" }}>
                          {
                            typeof item.value === "string" &&
                              item.value === "NA" ? (
                              "NA"
                            ) : (
                              <Chip
                                style={{
                                  backgroundColor: (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "#EAFFFE"
                                    : "#FFEDF3",
                                }}
                                icon={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  ) ? (
                                    <VerifiedOutlinedIcon />
                                  ) : (
                                    <ErrorOutlineOutlinedIcon />
                                  )
                                }
                                label={
                                  item.hasOwnProperty("custom_name") &&
                                    item.custom_name !== ""
                                    ? item.custom_name
                                    : typeof item.value === "string"
                                      ? item.value
                                      : item.value.toFixed(2)
                                }
                                variant="outlined"
                                color={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "success"
                                    : "error"
                                }
                                size="small"
                              />
                            )
                            // :
                            //   (
                            // <Chip
                            //   style={{
                            //     backgroundColor:
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? "#EAFFFE" : "#FFEDF3",
                            //   }}
                            //   icon={
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? (
                            //       <VerifiedOutlinedIcon />
                            //     ) : (
                            //       <ErrorOutlineOutlinedIcon />
                            //     )
                            //   }
                            //   label={typeof(item.value)==="string" ? item.value : item.value.toFixed(2)}
                            //   variant="outlined"
                            //   color={( item.hasOwnProperty('status') ? item.status : true ) ? "success" : "error"}
                            //   size="small"
                            // />
                            // )
                          }
                        </TableCell>
                      )
                  )}

                {props.evaluationData.evaluation_records.length > 0 &&
                  Object.keys(row).includes("static_metrics") &&
                  row.static_metrics.map(
                    (item) =>
                      !(["latency", "totalcost"].indexOf(item.name) > -1) && (
                        <TableCell style={{ position: "relative" }}>
                          {
                            typeof item.value === "string" &&
                              item.value === "NA" ? (
                              "NA"
                            ) : (
                              <Chip
                                style={{
                                  backgroundColor: (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "#EAFFFE"
                                    : "#FFEDF3",
                                }}
                                icon={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  ) ? (
                                    <VerifiedOutlinedIcon />
                                  ) : (
                                    <ErrorOutlineOutlinedIcon />
                                  )
                                }
                                label={
                                  item.hasOwnProperty("custom_name") && item.custom_name !== "" ? item.custom_name :
                                    typeof item.value === "string"
                                      ? item.value
                                      : item.value.toFixed(2)
                                }
                                variant="outlined"
                                color={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "success"
                                    : "error"
                                }
                                size="small"
                              />
                            )
                            // :
                            //   (
                            // <Chip
                            //   style={{
                            //     backgroundColor:
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? "#EAFFFE" : "#FFEDF3",
                            //   }}
                            //   icon={
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? (
                            //       <VerifiedOutlinedIcon />
                            //     ) : (
                            //       <ErrorOutlineOutlinedIcon />
                            //     )
                            //   }
                            //   label={typeof(item.value)==="string" ? item.value : item.value.toFixed(2)}
                            //   variant="outlined"
                            //   color={( item.hasOwnProperty('status') ? item.status : true ) ? "success" : "error"}
                            //   size="small"
                            // />
                            // )
                          }
                        </TableCell>
                      )
                  )}

                {props.evaluationData.evaluation_records.length > 0 &&
                  row.metadata &&
                  Object.entries(row.metadata).map(
                    (item) =>
                      ["latency", "cost"].indexOf(item[0]) === -1 && (
                        <TableCell>{item[1]}</TableCell>
                      )
                  )}
              </TableRow>
            );
          })}
      </TableBody>
    </>
  );
  if (ctx.projectSubType === "SUM" || ctx.projectSubType === "CODEGEN") {
    content = (
      <TableBody>
        {props.evaluationData.evaluation_records
          .slice(page * rowPage, page * rowPage + rowPage)
          .map((row, index) => {
            const uniqueRowNumber = page * rowPage + index + 1;
            return (
              <TableRow
                key={index}
                style={
                  index % 2
                    ? { background: "#F6F6F6" }
                    : { background: "white" }
                }
              >
                <TableCell
                  className={`${styles.stickyCol} ${styles.firstCol}`}
                  style={
                    index % 2
                      ? { background: "#F6F6F6" }
                      : { background: "white" }
                  }
                >
                  <Link
                    to={"sum-details/" + row._id}
                    state={{ num: uniqueRowNumber }}
                  >
                    {uniqueRowNumber}
                  </Link>
                </TableCell>
                {props.evaluationData &&
                  !(props.evaluationData.evaluation.constructor === Object) && (
                    <>
                      <TableCell
                        className={`${styles.stickyCol} ${styles.stickyCol2}`}
                        style={
                          index % 2
                            ? { background: "#F6F6F6" }
                            : { background: "white" }
                        }
                      >
                        {row.question.length > 50 ? (
                          <Typography
                            aria-owns={open ? "mouse-over-popover" : undefined}
                            aria-haspopup="true"
                            onMouseEnter={(ev) =>
                              handlePopoverOpen(ev, row.question)
                            }
                            onMouseLeave={handlePopoverClose}
                          >
                            {row.question.substr(0, 50) + "..."}
                          </Typography>
                        ) : (
                          <Typography>{row.question}</Typography>
                        )}
                      </TableCell>

                      <TableCell
                        className={`${styles.stickyCol} ${styles.stickyCol3}`}
                        style={
                          index % 2
                            ? { background: "#F6F6F6" }
                            : { background: "white" }
                        }
                      >
                        {row.answer.length > 50 ? (
                          <Typography
                            aria-owns={open ? "mouse-over-popover" : undefined}
                            aria-haspopup="true"
                            onMouseEnter={(ev) => {
                              let dt = ctx.projectSubType === "SUM" ? JSON.parse(row.answer) : row.answer;
                              handlePopoverOpen(ev, dt);
                            }}
                            onMouseLeave={handlePopoverClose}
                          >
                            {row.answer.substr(0, 50) + "..."}
                          </Typography>
                        ) : (
                          <Typography>{row.answer}</Typography>
                        )}
                      </TableCell>
                      {/* <TableCell
                        className={`${styles.stickyCol} ${styles.stickyCol4}`}
                        style={
                          index % 2
                            ? { background: "#F6F6F6" }
                            : { background: "white" }
                        }
                      >
                        Actions Completed
                      </TableCell> */}
                    </>
                  )}

                {props.evaluationData.evaluation_records.length > 0 &&
                  row.metrics.map(
                    (item) =>
                      !(["latency", "totalcost"].indexOf(item.name) > -1) && (
                        <TableCell style={{ position: "relative" }}>
                          {
                            typeof item.value === "string" &&
                              item.value === "NA" ? (
                              "NA"
                            ) : (
                              <Chip
                                style={{
                                  backgroundColor: (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "#EAFFFE"
                                    : "#FFEDF3",
                                }}
                                icon={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  ) ? (
                                    <VerifiedOutlinedIcon />
                                  ) : (
                                    <ErrorOutlineOutlinedIcon />
                                  )
                                }
                                label={
                                  typeof item.value === "string"
                                    ? item.value
                                    : item.value.toFixed(2)
                                }
                                variant="outlined"
                                color={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "success"
                                    : "error"
                                }
                                size="small"
                              />
                            )
                            // :
                            //   (
                            // <Chip
                            //   style={{
                            //     backgroundColor:
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? "#EAFFFE" : "#FFEDF3",
                            //   }}
                            //   icon={
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? (
                            //       <VerifiedOutlinedIcon />
                            //     ) : (
                            //       <ErrorOutlineOutlinedIcon />
                            //     )
                            //   }
                            //   label={typeof(item.value)==="string" ? item.value : item.value.toFixed(2)}
                            //   variant="outlined"
                            //   color={( item.hasOwnProperty('status') ? item.status : true ) ? "success" : "error"}
                            //   size="small"
                            // />
                            // )
                          }
                        </TableCell>
                      )
                  )}

                {props.evaluationData.evaluation_records.length > 0 &&
                  !(row.custom_metrics.constructor === Object) &&
                  row.custom_metrics.map(
                    (item) =>
                      !(["latency", "totalcost"].indexOf(item.name) > -1) && (
                        <TableCell style={{ position: "relative" }}>
                          {
                            typeof item.value === "string" &&
                              item.value === "NA" ? (
                              "NA"
                            ) : (
                              <Chip
                                style={{
                                  backgroundColor: (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "#EAFFFE"
                                    : "#FFEDF3",
                                }}
                                icon={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  ) ? (
                                    <VerifiedOutlinedIcon />
                                  ) : (
                                    <ErrorOutlineOutlinedIcon />
                                  )
                                }
                                label={
                                  item.hasOwnProperty("custom_name") && item.custom_name !== "" ? item.custom_name :
                                    typeof item.value === "string"
                                      ? item.value
                                      : item.value.toFixed(2)
                                }
                                variant="outlined"
                                color={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "success"
                                    : "error"
                                }
                                size="small"
                              />
                            )
                            // :
                            //   (
                            // <Chip
                            //   style={{
                            //     backgroundColor:
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? "#EAFFFE" : "#FFEDF3",
                            //   }}
                            //   icon={
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? (
                            //       <VerifiedOutlinedIcon />
                            //     ) : (
                            //       <ErrorOutlineOutlinedIcon />
                            //     )
                            //   }
                            //   label={typeof(item.value)==="string" ? item.value : item.value.toFixed(2)}
                            //   variant="outlined"
                            //   color={( item.hasOwnProperty('status') ? item.status : true ) ? "success" : "error"}
                            //   size="small"
                            // />
                            // )
                          }
                        </TableCell>
                      )
                  )}

                {/* {props.evaluationData.evaluation_records.length > 0 &&
                  row.custom_metrics &&
                  Object.keys(row.custom_metrics).map((item) => (
                    <TableCell>
                      {typeof row.custom_metrics[item].final_output ===
                      "string" ? (
                        row.custom_metrics[item].final_output
                      ) : (
                        <Chip
                          style={{
                            backgroundColor: (
                              row.custom_metrics[item].hasOwnProperty("status")
                                ? row.custom_metrics[item].status === "Pass"
                                : true
                            )
                              ? "#EAFFFE"
                              : "#FFEDF3",
                          }}
                          icon={
                            (
                              row.custom_metrics[item].hasOwnProperty("status")
                                ? row.custom_metrics[item].status === "Pass"
                                : true
                            ) ? (
                              <VerifiedOutlinedIcon />
                            ) : (
                              <ErrorOutlineOutlinedIcon />
                            )
                          }
                          label={
                            typeof row.custom_metrics[item].hasOwnProperty(
                              "custom_name"
                            )
                              ? row.custom_metrics[item].custom_name
                              : row.custom_metrics[item].final_output.toFixed(2)
                          }
                          variant="outlined"
                          color={
                            (
                              row.custom_metrics[item].hasOwnProperty("status")
                                ? row.custom_metrics[item].status === "Pass"
                                : true
                            )
                              ? "success"
                              : "error"
                          }
                          size="small"
                        />
                      )}
                    </TableCell>
                  ))} */}

                {props.evaluationData.evaluation_records.length > 0 &&
                  Object.keys(row).includes("static_metrics") &&
                  row.static_metrics.map(
                    (item) =>
                      !(["latency", "totalcost"].indexOf(item.name) > -1) && (
                        <TableCell style={{ position: "relative" }}>
                          {
                            typeof item.value === "string" &&
                              item.value === "NA" ? (
                              "NA"
                            ) : (
                              <Chip
                                style={{
                                  backgroundColor: (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "#EAFFFE"
                                    : "#FFEDF3",
                                }}
                                icon={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  ) ? (
                                    <VerifiedOutlinedIcon />
                                  ) : (
                                    <ErrorOutlineOutlinedIcon />
                                  )
                                }
                                label={
                                  item.hasOwnProperty("custom_name") && item.custom_name !== "" ? item.custom_name :
                                    typeof item.value === "string"
                                      ? item.value
                                      : item.value.toFixed(2)
                                }
                                variant="outlined"
                                color={
                                  (
                                    item.hasOwnProperty("status")
                                      ? item.status
                                      : true
                                  )
                                    ? "success"
                                    : "error"
                                }
                                size="small"
                              />
                            )
                            // :
                            //   (
                            // <Chip
                            //   style={{
                            //     backgroundColor:
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? "#EAFFFE" : "#FFEDF3",
                            //   }}
                            //   icon={
                            //     ( item.hasOwnProperty('status') ? item.status : true ) ? (
                            //       <VerifiedOutlinedIcon />
                            //     ) : (
                            //       <ErrorOutlineOutlinedIcon />
                            //     )
                            //   }
                            //   label={typeof(item.value)==="string" ? item.value : item.value.toFixed(2)}
                            //   variant="outlined"
                            //   color={( item.hasOwnProperty('status') ? item.status : true ) ? "success" : "error"}
                            //   size="small"
                            // />
                            // )
                          }
                        </TableCell>
                      )
                  )}

                {props.evaluationData.evaluation_records.length > 0 &&
                  row.metadata &&
                  Object.entries(row.metadata).map(
                    (item) =>
                      ["latency", "cost"].indexOf(item[0]) === -1 && (
                        <TableCell>{item[1]}</TableCell>
                      )
                  )}
              </TableRow>
            );
          })}
      </TableBody>
    );
  }

  return (
    <div>
      <div className="tab-container">
        <div className="tab-buttons">
          <button
            className={activeTab === 'table' ? 'active' : ''}
            onClick={() => setActiveTab('table')}
          >
            <img src={listIcon} />&nbsp;
            Table view
          </button>
          <button
            className={activeTab === 'arial' ? 'active' : ''}
            onClick={() => setActiveTab('arial')}
          >
            <img src={deselectIcon} /> &nbsp;
            Analysis View
          </button>
        </div>
        <div className="tab-content">
          {/* {activeTab === 'spread' ? renderTable(spreadData) : renderTable(irrelevantData)} */}
        </div>
      </div>
      <Popover
        id="mouse-over-popover"
        sx={{
          pointerEvents: "none",
          width: "70rem",
        }}
        open={open}
        anchorEl={anchorEl}
        anchorOrigin={{
          vertical: "center",
          horizontal: "left",
        }}
        transformOrigin={{
          vertical: "top",
          horizontal: "left",
        }}
        onClose={handlePopoverClose}
        disableRestoreFocus
      >
        <div style={{ padding: "1rem" }}>
          {typeof hoverData === "object" ? (
            <div>
              {Object.keys(hoverData).map((it) => (
                <div>
                  <div
                    style={{
                      fontWeight: 500,
                      width: "100%",
                    }}
                  >
                    {it}
                  </div>
                  <p style={{ fontWeight: 300, margin: "0.5rem 0" }}>
                    {hoverData[it]}
                  </p>
                </div>
              ))}
            </div>
          ) : (
            hoverData
          )}{" "}
        </div>
      </Popover>
      {/* <Button style={{marginBottom: "1rem"}} startIcon={<KeyboardBackspaceIcon/>} variant="outlined" onClick={()=>navigate("/genai-assurance/genai_evaluate")}>Back</Button> */}
      {activeTab === 'table' && (<div className="filter-container"><DropdownFilter
        data={sampleFilterData}
        selectedTopic={selectedTopic}
        selectedSubTopics={selectedSubTopics}
        onTopicChange={handleTopicChange}
        onSubTopicChange={handleSubTopicChange}
        applyFilterData={handleApplyFilter}
      />
      </div>)}
      {activeTab === 'table' && (<TableContainer
        component={Paper}
        style={{ width: "100%", overflowX: "auto", marginTop: "35px" }}
      >
        <Table aria-label="Material-UI Table" stickyHeader>
          <TableHead sx={{ background: "#111270", color: "#fff" }}>
            <TableRow
              sx={{
                background: "#111270",
                alignItems: "center",
                color: "#fff",
              }}
            >
              <TableCell
                className={`${styles.stickyCol} ${styles.firstCol}`}
                sx={{
                  background: "#111270",
                  color: "#fff",
                  textAlign: "left",
                  zIndex: 10,
                }}
              >
                <strong>S.No.</strong>
              </TableCell>

              {props.evaluationData &&
                !(props.evaluationData.evaluation.constructor === Object) && (
                  <>
                    <TableCell
                      className={`${styles.stickyCol} ${styles.stickyCol2}`}
                      sx={{
                        width: "10rem",
                        background: "#111270",
                        color: "#fff",
                        textAlign: "left",
                        zIndex: 10,
                      }}
                    >
                      <strong>Input</strong>
                    </TableCell>
                    <TableCell
                      className={`${styles.stickyCol} ${styles.stickyCol3}`}
                      sx={{
                        background: "#111270",
                        color: "#fff",
                        textAlign: "left",
                        zIndex: 10,
                      }}
                    >
                      <strong>Output</strong>
                    </TableCell>
                    {/* {ctx.projectSubType === "SUM" && (
                      <TableCell
                        className={`${styles.stickyCol} ${styles.stickyCol4}`}
                        sx={{
                          background: "#111270",
                          color: "#fff",
                          textAlign: "left",
                          zIndex: 10,
                        }}
                      >
                        <strong>Action</strong>
                      </TableCell>
                    )} */}
                  </>
                )}

              {props.evaluationData &&
                props.evaluationData.evaluation.constructor === Object && (
                  <>
                    <TableCell
                      className={`${styles.stickyCol} ${styles.stickyCol2}`}
                      sx={{
                        background: "#111270",
                        color: "#fff",
                        textAlign: "left",
                        zIndex: 10,
                      }}
                    >
                      <strong>Input</strong>
                    </TableCell>
                    <TableCell
                      className={`${styles.stickyCol} ${styles.stickyCol3}`}
                      sx={{
                        background: "#111270",
                        color: "#fff",
                        textAlign: "left",
                        zIndex: 10,
                      }}
                    >
                      <strong>Output</strong>
                    </TableCell>
                    <TableCell
                      className={`${styles.stickyCol} ${styles.stickyCol4}`}
                      sx={{
                        background: "#111270",
                        color: "#fff",
                        textAlign: "left",
                        zIndex: 10,
                      }}
                    >
                      <strong>Context</strong>
                    </TableCell>
                  </>
                )}

              {/* {ctx.projectSubType === "SUM" &&
                props.evaluationData.evaluation_records.length > 0 &&
                props.evaluationData.evaluation_records[0].metrics.map(
                  (item) =>
                    !(["latency", "totalcost"].indexOf(item.name) > -1) && (
                      <TableCell
                        sx={{
                          background: "#111270",
                          position: "relative",
                          color: "#fff",
                          textAlign: "left",
                        }}
                      >
                        <strong>{item.name}</strong>
                      </TableCell>
                    )
                )} */}

              {props.evaluationData.evaluation_records.length > 0 &&
                props.evaluationData.evaluation_records[0].metrics.map(
                  (item) =>
                    !(["latency", "totalcost"].indexOf(item.name) > -1) && (
                      <TableCell
                        sx={{
                          background: "#111270",
                          position: "relative",
                          color: "#fff",
                          textAlign: "left",
                        }}
                      >
                        <strong>{item.name}</strong>
                      </TableCell>
                    )
                )}

              {props.evaluationData.evaluation_records.length > 0 &&
                !(
                  props.evaluationData.evaluation_records[0].custom_metrics
                    .constructor === Object
                ) &&
                props.evaluationData.evaluation_records[0].custom_metrics &&
                props.evaluationData.evaluation_records[0].custom_metrics.map(
                  (item) => (
                    <TableCell
                      sx={{
                        background: "#111270",
                        color: "#fff",
                        textAlign: "left",
                      }}
                    >
                      <strong>{item.name}</strong>
                    </TableCell>
                  )
                )}

              {props.evaluationData.evaluation_records.length > 0 &&
                props.evaluationData.evaluation_records[0].static_metrics &&
                props.evaluationData.evaluation_records[0].static_metrics.map(
                  (item) => (
                    <TableCell
                      sx={{
                        background: "#111270",
                        color: "#fff",
                        textAlign: "left",
                      }}
                    >
                      <strong>{item.name}</strong>
                    </TableCell>
                  )
                )}

              {props.evaluationData.evaluation_records.length > 0 &&
                props.evaluationData.evaluation_records[0].metadata &&
                Object.keys(
                  props.evaluationData.evaluation_records[0].metadata
                ).map(
                  (item) =>
                    ["latency", "cost"].indexOf(item) === -1 && (
                      <TableCell
                        sx={{
                          background: "#111270",
                          color: "#fff",
                          textAlign: "left",
                        }}
                      >
                        <strong>{item.toUpperCase()}</strong>
                      </TableCell>
                    )
                )}
            </TableRow>
          </TableHead>
          {props.loading === true ? (
            <TableBody>
              <TableRow>
                <TableCell colSpan={8}>
                  <Stack width="100%" direction="row" justifyContent="center">
                    <CircularProgress />
                  </Stack>
                </TableCell>
              </TableRow>
            </TableBody>
          ) : props.evaluationData.evaluation_records.length === 0 ? (
            <TableBody>
              <TableRow>
                <TableCell colSpan={8}>
                  <Stack width="100%" direction="row" justifyContent="center">
                    No Data Available
                  </Stack>
                </TableCell>
              </TableRow>
            </TableBody>
          ) : (
            content
          )}
        </Table>
      </TableContainer>)}
      {activeTab === 'table' && (<TablePagination
        rowsPerPageOptions={[5, 10, 25]}
        component="div"
        count={props.evaluationData.evaluation_records.length}
        rowsPerPage={rowPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />)}

      {activeTab === 'arial' && (<div className="analysis-view-container">
        <AggregatedScore />
        <AugmentationInsights/>
        <EntityBasedInsights/>
        <ScoreDistribution/>
      </div>)}
    </div>
  );
};

export default TelemetryMetrices;