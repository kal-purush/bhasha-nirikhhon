const initialState = {
    _id: 0,
    home: {
        headerTitle: "",
        headerBody: "", 
        headerButton: "", 
        activitiesTitle: "",        
        instructorsTitle: "",
        tracksTitle: "",
        activities: [
            {activityIcon: "", activityTitle: "", activityContent: ""},
            {activityIcon: "", activityTitle: "", activityContent: ""},
            {activityIcon: "", activityTitle: "", activityContent: ""},
            {activityIcon: "", activityTitle: "", activityContent: ""},
            {activityIcon: "", activityTitle: "", activityContent: ""}
        ],
        instructors: [
            {instructorName: "", instructorTrack: "", instructorImage: ""},
            {instructorName: "", instructorTrack: "", instructorImage: ""},
            {instructorName: "", instructorTrack: "", instructorImage: ""},
            {instructorName: "", instructorTrack: "", instructorImage: ""}
          ],
        tracks: [
            {trackHeader: "", trackText: ""},
            {trackHeader: "", trackText: ""},
            {trackHeader: "", trackText: ""},
            {trackHeader: "", trackText: ""},
            {trackHeader: "", trackText: ""}
          ]
    },
    mission: {
        headerTitle: "", 
        headerBody: "", 
        headerButton: "", 
        tracksHeader: "", 
        tracksText: "", 
        tracks: [
            {trackTitle: "", trackContent: "", trackImage: ""},
            {trackTitle: "", trackContent: "", trackImage: ""},
            {trackTitle: "", trackContent: "", trackImage: ""},
            {trackTitle: "", trackContent: "", trackImage: ""}
        ],
        footerButton: "",
        footerText: "",
        missionContentA: "",
        missionContentB: ""
    },
    tutors: {
        details: [
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
            {tutorName: "", tutorTrack: "", tutorImage: "", githubInfo: "", twitterInfo: ""},
          ],
        detailsTitle: "",
        headerBody: "",
        headerTitle: "",
    },
    pastInterns: {
        details: [
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
            {internName: "", internTrack: "", internImage: ""},
          ],
        detailsTitle: "",
        headerText: "",
    },
    about: {
        benefits: [
            {benefitTitle: "", benefitText: ""},
            {benefitTitle: "", benefitText: ""},
            {benefitTitle: "", benefitText: ""},
            {benefitTitle: "", benefitText: ""},
            {benefitTitle: "", benefitText: ""}
          ],
        detailsImage: "",
        detailsText: "",
        detailsTitle: "",
        headerText: "",
        headerTitle: "",
    },
    footer: {
        emailIcon: "",
        emailText: "",
        footerImage: "",
        footerText: "",
        footerText2: "",
        icons: [
            {iconLink: "", mediaSrc: ""},
            {iconLink: "", mediaSrc: ""},
            {iconLink: "", mediaSrc: ""}
          ]
    },
    manager: {
        instagramInfo: "",
        managerImage: "",
        managerName: "",
        managerText: "",
        managerTitle: "",
        twitterInfo: ""
    },
    registrationForm: {
        formButton: "",
        formHeader: "",
        formTitle: "",
        surveyHeader: "",
        surveyIntro: "",
        surveyIntro1: ""
    }
}

function Reducer(state = initialState, action) {
    switch (action.type) {
        case "UPDATE_STATE":
            return {...state, ...action.data}
        default:
            return initialState
    }
}

export default Reducer