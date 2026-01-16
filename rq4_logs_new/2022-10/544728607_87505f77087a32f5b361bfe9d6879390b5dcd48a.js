import React ,{useState} from 'react'
import Collapsible from 'react-collapsible'
import CollapsibleQuestionStyle from "./CollapsibleQuestionStyle.module.css"

export default function CollapsibleQuestion(props) {

    const [questionStatus, setQuestionStatus] = useState("")

    function TriggerElement() {
        return (
            <div className={CollapsibleQuestionStyle.trigger_element}>
                {questionStatus}
            </div>
        )
    }

    function UngroupQuestion() {
        let questionResult = ""
        if (props.selectedAnswer === props.correctAnswer) {
            questionResult = "correct"
        } else {
            questionResult = "incorrect"
        }

        setQuestionStatus(questionResult)

        return (
            <div>
                <p>
                    <strong>Question:</strong> {props.question}
                </p>
                <p>
                    <strong>Your Answer:</strong> {props.selectedAnswer}
                </p>
                <p>
                    <strong>Correct Answer:</strong> {props.correctAnswer}
                </p>
                <p>
                    <strong>Explanation:</strong> {props.explanation}
                </p>
            </div>
        )
    }

    function GroupQuestion() {

        let questionResult = ""

        const questions = props.questions.map(questionData => {
            if (questionData.selectedAnswer === questionData.correctAnswer) {
                questionResult += " correct"
            } else {
               questionResult += " incorrect"
            }
            return (
                <div>
                    <hr></hr>
                    <p>
                        <strong>Question:</strong> {questionData.question}
                    </p>
                    <p>
                        <strong>Your Answer:</strong> {questionData.selectedAnswer}
                    </p>
                    <p>
                        <strong>Correct Answer:</strong> {questionData.correctAnswer}
                    </p>
                    <p>
                        <strong>Explanation:</strong> {questionData.explanation}
                    </p>
                    <hr></hr>
            </div>
            )
        })
        setQuestionStatus(questionResult)
        return (
            <div>
                {/* <p>Reference: {props.questionReference}</p> */}
   
                {props.questionReference.includes("https") ? (
                    <img
                    src={`${props.questionReference}`}
                    alt='question reference'
                    style={{
                        width: '90%'
                    }}
                    />
                ) : (
                    <p style={{ whiteSpace: 'pre-line' }}>{props.questionReference}</p>
                )}
                <div>
                    {questions}
                </div>
            </div>
        )
    }

    function CollapsibleContent() {
        if (props.hasOwnProperty("questionReference")) {
            return (<GroupQuestion />)
        } else {
            return (<UngroupQuestion />)
        }
    }

    return (
        <Collapsible trigger={<TriggerElement />} transitionTime={ 200}>
            <div className={CollapsibleQuestionStyle.collasible_container}>
                <CollapsibleContent />
            </div>
        </Collapsible>
    )
}