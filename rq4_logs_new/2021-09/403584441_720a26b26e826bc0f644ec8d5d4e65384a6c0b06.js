import * as t from "../Types";

const initialState = {
    classDetails: -1,
    classSaveBtn: false,
    classdata: [
        { classNumber: "1-b", teacher: "Sardor Safarov", numberOfStudents: "25" },
        { classNumber: "2-b", teacher: "Sardor Safarov", numberOfStudents: "17" },
        { classNumber: "3-b", teacher: "Sardor Safarov", numberOfStudents: "20" },
        { classNumber: "4-b", teacher: "Sardor Safarov", numberOfStudents: "27" },
    ]
}

const ClassReducer = (state = initialState, action) => {
    const classdata = [...state.classdata];
    switch (action.type) {
        case t.SHOW_CLASS_DETAILS: return { ...state, classDetails: action.payload };
        case t.SAVE_CLASS:
            classdata[action.index].classNumber = action.payloadv1;
            classdata[action.index].teacher = action.payloadv2;
            classdata[action.index].numberOfStudents = action.payloadv3;
            return { ...state, classSaveBtn: action.payload, classdata: classdata, classSaveBtn: false };
        case t.SAVE_CLASS_BTN: return { ...state, classSaveBtn: true }
        case t.DELETE_CLASS: classdata.splice(action.payload, 1);
            return { ...state, classdata: classdata }
        default: return state;
    }
}

export default ClassReducer;