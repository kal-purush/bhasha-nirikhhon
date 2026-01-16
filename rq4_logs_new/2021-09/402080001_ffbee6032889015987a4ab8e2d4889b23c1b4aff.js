import React from "react";
import Layout from "../components/Layout";
import { Typography } from "@material-ui/core";

const pageStyles = {
    backgroundColor: "#f89773",
    padding: "1rem",
    width: "100vw",
};

const Schedule = () => {
    return (
        <Layout pageTitle="Schedule">
            <div style={pageStyles}>
                <h1>View our schedule</h1>
                <br />
                <Typography>Import Timetable here</Typography>
            </div>
        </Layout>
    );
};

export default Schedule;