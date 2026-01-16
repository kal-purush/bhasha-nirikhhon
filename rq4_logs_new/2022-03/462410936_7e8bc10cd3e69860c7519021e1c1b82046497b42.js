const initialState = {
    loadQuiz:[]
}

const QuizReducer = (state = initialState ,action) => {
    switch(action.type) {
        case "LOAD-QUIZ":
            return{
                ...state,
                loadQuiz: action.payload
            }

        default:
            return state
    }
}

export default QuizReducer;