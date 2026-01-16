import React from "react";
import JOOBLE_data from './JOOBLE.json'
import USA_JOBS_data from './USA_JOBS.json'

function RenderSearch(){
    return(
        <div>
            {{JOOBLE_data}}{{USA_JOBS_data}}
        </div>
    )
}

export default RenderSearch;