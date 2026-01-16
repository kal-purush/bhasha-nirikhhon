import React, {useEffect} from "react";
import QuestionList from "../../../containers/questions/QuestionList";
import FilterPanel from "./FIlterPanel";

const  Questions  = ({ fetchQuestions }) => {

    useEffect(() => {
       fetchQuestions();
    });

    return (
        <>
            <FilterPanel/>

            <QuestionList/>
        </>
    )
};

export default Questions;