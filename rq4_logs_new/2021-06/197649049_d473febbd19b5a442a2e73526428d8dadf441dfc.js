// @flow

import ReactDOM from "react-dom";
import CoursePage from "./smartComponents/course/Course";
import React from "react";
import {InstanceError} from "../rules/ErrorHandler/InstanceError";
import {ServerStyleSheets} from "@material-ui/core/styles";
import ReactDOMServer from "react-dom/server";
import template from "./template";
import AppData from "../rules/AppData";
import type {Result, Run, RunProps} from "../rules/App";

export class ReactApp {

    static course: Run = ({dataSource, loggerClient}) => {
        const App = () => <CoursePage dataSource={dataSource} loggerClient={loggerClient}/>
        const Root = document.getElementById('root')
        if (!Root) throw InstanceError.create("Root")
        ReactDOM.hydrate(<App/>, Root);
        return {}
    }

    static courseServerSide(props: RunProps): Result {
        const {dataSource, loggerClient} = props
        const sheets = new ServerStyleSheets()
        const component = <CoursePage dataSource={dataSource} loggerClient={loggerClient}/>
        const body = ReactDOMServer.renderToString(sheets.collect(component))
        const appData = AppData.createCourseApp({
            course: dataSource.course.replica, profile: dataSource.profile.replica
        })
        const html = template({app: body, css: sheets.toString(), appData: appData, entryPoint: "course"})
        return {html}
    }
}