import React, { useContext } from 'react'
import {Link} from 'react-router-dom'
import JobsContext from '../context/jobs-context'
const JobsOpening = ( {job} ) => {

    return(
        <div className = 'row job_opening'>
            <div key = {job.id} className = 'job_description flex-grow'><strong>
            <Link to = {job.url}>{job.company}</Link></strong> - {job.title} ({job.type}) @ {job.location}
            </div>
            <div className = 'btn btn-success'>I Applied</div>
            <a href = {job.url} target ='_blank' rel = 'noopener noreferrer'>
            <div className = 'btn btn-more'>See More</div>
            </a>

                    
        </div>
    )

}
export default JobsOpening