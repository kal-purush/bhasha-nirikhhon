"""feature / subfeature -> search tag mapping for course_features"""

FEATURE_AV_FACULTY_INTRODUCTIONS = "AV faculty introductions"
FEATURE_AV_LECTURES = "AV lectures"
FEATURE_AV_RECITATIONS = "AV recitations"
FEATURE_AV_SELECTED_LECTURES = "AV selected lectures"
FEATURE_AV_SPECIAL_ELEMENT_AUDIO = "AV special element audio"
FEATURE_AV_SPECIAL_ELEMENT_VIDEO = "AV special element video"
FEATURE_ASSIGNMENTS = "Assignments"
FEATURE_EXAMS = "Exams"
FEATURE_IMAGE_GALLERY = "Image Gallery"
FEATURE_INSTRUCTOR_INSIGHTS = "Instructor Insights"
FEATURE_INTERACTIVE_ASSESSMENTS = "Interactive assessments"
FEATURE_INTERACTIVE_SIMULATIONS = "Interactive simulations"
FEATURE_SIMULATIONS = "Simulations"
FEATURE_LECTURE_NOTES = "Lecture notes"
FEATURE_ONLINE_TEXTBOOKS = "Online textbooks"
FEATURE_PROJECTS = "Projects"
FEATURE_RESOURCE_INDEX = "Resource Index"
FEATURE_THIS_COURSE_AT_MIT = "This Course at MIT"

SUBFEATURE_NONE = ""
SUBFEATURE_VIDEO = "Video"
SUBFEATURE_AUDIO = "Audio"
SUBFEATURE_GUEST_LECTURE = "Guest lecture"
SUBFEATURE_OTHER = "Other"
SUBFEATURE_DEMONSTRATION = "Demonstration"
SUBFEATURE_MUSIC = "Music"
SUBFEATURE_FIELD_TRIP = "Field Trip"
SUBFEATURE_COMPETITION = "Competition"
SUBFEATURE_WORKSHOP = "Workshop"
SUBFEATURE_TUTORIAL = "Tutorial"
SUBFEATURE_WITH_SOLUTIONS_EXAMPLES = "with solutions / examples"
SUBFEATURE_ACTIVITY_NO_EXAMPLES = "activity (no examples)"
SUBFEATURE_ACTIVITY_WITH_EXAMPLES = "activity with examples"
SUBFEATURE_DESIGN_NO_EXAMPLES = "design (no examples)"
SUBFEATURE_DESIGN_WITH_EXAMPLES = "design with examples"
SUBFEATURE_MEDIA_NO_EXAMPLES = "media (no examples)"
SUBFEATURE_MEDIA_WITH_EXAMPLES = "media with examples"
SUBFEATURE_PRESENTATIONS_NO_EXAMPLES = "presentations (no examples)"
SUBFEATURE_PRESENTATIONS_WITH_EXAMPLES = "presentations with examples"
SUBFEATURE_PROBLEM_SETS_NO_SOLUTIONS = "problem sets (no solutions)"
SUBFEATURE_PROBLEM_SETS_WITH_SOLUTIONS = "problem sets with solutions"
SUBFEATURE_PROGRAMMING_NO_EXAMPLES = "programming (no examples)"
SUBFEATURE_PROGRAMMING_WITH_EXAMPLES = "programming with examples"
SUBFEATURE_WRITTEN_NO_EXAMPLES = "written (no examples)"
SUBFEATURE_WRITTEN_WITH_EXAMPLES = "written with examples"
SUBFEATURE_NO_SOLUTIONS = "No solutions"
SUBFEATURE_SOLUTIONS = "Solutions"
SUBFEATURE_APPLET = "Applet"
SUBFEATURE_SELECTED = "Selected"
SUBFEATURE_COMPLETE = "Complete"
SUBFEATURE_MATHML = "MathML"
SUBFEATURE_HTML = "HTML"
SUBFEATURE_PDF = "PDF"
SUBFEATURE_EXAMPLES = "Examples"
SUBFEATURE_NO_EXAMPLES = "No examples"

TAG_COURSE_INTRODUCTION = "Course Introduction"
TAG_LECTURE_VIDEOS = "Lecture Videos"
TAG_LECTURE_AUDIO = "Lecture Audio"
TAG_RECITATION_VIDEOS = "Recitation Videos"
TAG_OTHER_AUDIO = "Other Audio"
TAG_DEMONSTRATION_AUDIO = "Demonstration Audio"
TAG_MUSIC = "Music"
TAG_OTHER_VIDEO = "Other Video"
TAG_DEMONSTRATION_VIDEOS = "Demonstration Videos"
TAG_VIDEO_MATERIALS = "Video Materials"
TAG_COMPETITION_VIDEOS = "Competition Videos"
TAG_WORKSHOP_VIDEOS = "Workshop Videos"
TAG_VIDEOS = "Videos"
TAG_TUTORIAL_VIDEOS = "Tutorial Videos"
TAG_MULTIPLE_ASSIGNMENT_TYPES = "Multiple Assignment Types"
TAG_MULTIPLE_ASSIGNMENT_TYPES_WITH_SOLUTIONS = (
    "Multiple Assignment Types with Solutions"
)
TAG_ACTIVITY_ASSIGNMENTS = "Activity Assignments"
TAG_ACTIVITY_ASSIGNMENTS_WITH_EXAMPLES = "Activity Assignments with Examples"
TAG_DESIGN_ASSIGNMENTS = "Design Assignments"
TAG_DESIGN_ASSIGNMENTS_WITH_EXAMPLES = "Design Assignments with Examples"
TAG_MEDIA_ASSIGNMENTS = "Media Assignments"
TAG_MEDIA_ASSIGNMENTS_WITH_EXAMPLES = "Media Assignments with Examples"
TAG_PRESENTATION_ASSIGNMENTS = "Presentation Assignments"
TAG_PRESENTATION_ASSIGNMENTS_WITH_EXAMPLES = "Presentation Assignments with Examples"
TAG_PROBLEM_SETS = "Problem Sets"
TAG_PROBLEM_SETS_WITH_SOLUTIONS = "Problem Sets with Solutions"
TAG_PROGRAMMING_ASSIGNMENTS = "Programming Assignments"
TAG_PROGRAMMING_ASSIGNMENTS_WITH_EXAMPLES = "Programming Assignments with Examples"
TAG_WRITTEN_ASSIGNMENTS = "Written Assignments"
TAG_WRITTEN_ASSIGNMENTS_WITH_EXAMPLES = "Written Assignments with Examples"
TAG_EXAM_MATERIALS = "Exam Materials"
TAG_EXAMS = "Exams"
TAG_EXAMS_WITH_SOLUTION = "Exams with Solution"
TAG_IMAGE_GALLERY = "Image Gallery"
TAG_NONE = ""
TAG_SIMULATIONS = "Simulations"
TAG_SIMULATION_VIDOES = "Simulation Vidoes"
TAG_LECTURE_NOTES = "Lecture Notes"
TAG_ONLINE_TEXTBOOK = "Online Textbook"
TAG_PROJECTS = "Projects"
TAG_PROJECTS_WITH_EXAMPLES = "Projects with Examples"

TAG_MAPPING = [
    {
        "ocw_feature": FEATURE_AV_FACULTY_INTRODUCTIONS,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_COURSE_INTRODUCTION,
    },
    {
        "ocw_feature": FEATURE_AV_FACULTY_INTRODUCTIONS,
        "ocw_subfeature": SUBFEATURE_VIDEO,
        "course_feature_tag": TAG_COURSE_INTRODUCTION,
    },
    {
        "ocw_feature": FEATURE_AV_LECTURES,
        "ocw_subfeature": SUBFEATURE_VIDEO,
        "course_feature_tag": TAG_LECTURE_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_AV_LECTURES,
        "ocw_subfeature": SUBFEATURE_AUDIO,
        "course_feature_tag": TAG_LECTURE_AUDIO,
    },
    {
        "ocw_feature": FEATURE_AV_RECITATIONS,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_RECITATION_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_AV_SELECTED_LECTURES,
        "ocw_subfeature": SUBFEATURE_AUDIO,
        "course_feature_tag": TAG_LECTURE_AUDIO,
    },
    {
        "ocw_feature": FEATURE_AV_SELECTED_LECTURES,
        "ocw_subfeature": SUBFEATURE_VIDEO,
        "course_feature_tag": TAG_LECTURE_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_AUDIO,
        "ocw_subfeature": SUBFEATURE_GUEST_LECTURE,
        "course_feature_tag": TAG_LECTURE_AUDIO,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_AUDIO,
        "ocw_subfeature": SUBFEATURE_OTHER,
        "course_feature_tag": TAG_OTHER_AUDIO,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_AUDIO,
        "ocw_subfeature": SUBFEATURE_DEMONSTRATION,
        "course_feature_tag": TAG_DEMONSTRATION_AUDIO,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_AUDIO,
        "ocw_subfeature": SUBFEATURE_MUSIC,
        "course_feature_tag": TAG_MUSIC,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_VIDEO,
        "ocw_subfeature": SUBFEATURE_OTHER,
        "course_feature_tag": TAG_OTHER_VIDEO,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_VIDEO,
        "ocw_subfeature": SUBFEATURE_DEMONSTRATION,
        "course_feature_tag": TAG_DEMONSTRATION_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_VIDEO,
        "ocw_subfeature": SUBFEATURE_FIELD_TRIP,
        "course_feature_tag": TAG_VIDEO_MATERIALS,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_VIDEO,
        "ocw_subfeature": SUBFEATURE_GUEST_LECTURE,
        "course_feature_tag": TAG_LECTURE_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_VIDEO,
        "ocw_subfeature": SUBFEATURE_COMPETITION,
        "course_feature_tag": TAG_COMPETITION_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_VIDEO,
        "ocw_subfeature": SUBFEATURE_WORKSHOP,
        "course_feature_tag": TAG_WORKSHOP_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_VIDEO,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_AV_SPECIAL_ELEMENT_VIDEO,
        "ocw_subfeature": SUBFEATURE_TUTORIAL,
        "course_feature_tag": TAG_TUTORIAL_VIDEOS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_MULTIPLE_ASSIGNMENT_TYPES,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_WITH_SOLUTIONS_EXAMPLES,
        "course_feature_tag": TAG_MULTIPLE_ASSIGNMENT_TYPES_WITH_SOLUTIONS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_ACTIVITY_NO_EXAMPLES,
        "course_feature_tag": TAG_ACTIVITY_ASSIGNMENTS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_ACTIVITY_WITH_EXAMPLES,
        "course_feature_tag": TAG_ACTIVITY_ASSIGNMENTS_WITH_EXAMPLES,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_DESIGN_NO_EXAMPLES,
        "course_feature_tag": TAG_DESIGN_ASSIGNMENTS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_DESIGN_WITH_EXAMPLES,
        "course_feature_tag": TAG_DESIGN_ASSIGNMENTS_WITH_EXAMPLES,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_MEDIA_NO_EXAMPLES,
        "course_feature_tag": TAG_MEDIA_ASSIGNMENTS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_MEDIA_WITH_EXAMPLES,
        "course_feature_tag": TAG_MEDIA_ASSIGNMENTS_WITH_EXAMPLES,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_PRESENTATIONS_NO_EXAMPLES,
        "course_feature_tag": TAG_PRESENTATION_ASSIGNMENTS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_PRESENTATIONS_WITH_EXAMPLES,
        "course_feature_tag": TAG_PRESENTATION_ASSIGNMENTS_WITH_EXAMPLES,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_PROBLEM_SETS_NO_SOLUTIONS,
        "course_feature_tag": TAG_PROBLEM_SETS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_PROBLEM_SETS_WITH_SOLUTIONS,
        "course_feature_tag": TAG_PROBLEM_SETS_WITH_SOLUTIONS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_PROGRAMMING_NO_EXAMPLES,
        "course_feature_tag": TAG_PROGRAMMING_ASSIGNMENTS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_PROGRAMMING_WITH_EXAMPLES,
        "course_feature_tag": TAG_PROGRAMMING_ASSIGNMENTS_WITH_EXAMPLES,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_WRITTEN_NO_EXAMPLES,
        "course_feature_tag": TAG_WRITTEN_ASSIGNMENTS,
    },
    {
        "ocw_feature": FEATURE_ASSIGNMENTS,
        "ocw_subfeature": SUBFEATURE_WRITTEN_WITH_EXAMPLES,
        "course_feature_tag": TAG_WRITTEN_ASSIGNMENTS_WITH_EXAMPLES,
    },
    {
        "ocw_feature": FEATURE_EXAMS,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_EXAM_MATERIALS,
    },
    {
        "ocw_feature": FEATURE_EXAMS,
        "ocw_subfeature": SUBFEATURE_NO_SOLUTIONS,
        "course_feature_tag": TAG_EXAMS,
    },
    {
        "ocw_feature": FEATURE_EXAMS,
        "ocw_subfeature": SUBFEATURE_SOLUTIONS,
        "course_feature_tag": TAG_EXAMS_WITH_SOLUTION,
    },
    {
        "ocw_feature": FEATURE_IMAGE_GALLERY,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_IMAGE_GALLERY,
    },
    {
        "ocw_feature": FEATURE_INSTRUCTOR_INSIGHTS,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_NONE,
    },
    {
        "ocw_feature": FEATURE_INTERACTIVE_ASSESSMENTS,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_NONE,
    },
    {
        "ocw_feature": FEATURE_INTERACTIVE_SIMULATIONS,
        "ocw_subfeature": SUBFEATURE_APPLET,
        "course_feature_tag": TAG_SIMULATIONS,
    },
    {
        "ocw_feature": FEATURE_INTERACTIVE_SIMULATIONS,
        "ocw_subfeature": SUBFEATURE_OTHER,
        "course_feature_tag": TAG_SIMULATIONS,
    },
    {
        "ocw_feature": FEATURE_SIMULATIONS,
        "ocw_subfeature": SUBFEATURE_OTHER,
        "course_feature_tag": TAG_SIMULATIONS,
    },
    {
        "ocw_feature": FEATURE_SIMULATIONS,
        "ocw_subfeature": SUBFEATURE_VIDEO,
        "course_feature_tag": TAG_SIMULATION_VIDOES,
    },
    {
        "ocw_feature": FEATURE_LECTURE_NOTES,
        "ocw_subfeature": SUBFEATURE_SELECTED,
        "course_feature_tag": TAG_LECTURE_NOTES,
    },
    {
        "ocw_feature": FEATURE_LECTURE_NOTES,
        "ocw_subfeature": SUBFEATURE_COMPLETE,
        "course_feature_tag": TAG_LECTURE_NOTES,
    },
    {
        "ocw_feature": FEATURE_ONLINE_TEXTBOOKS,
        "ocw_subfeature": SUBFEATURE_OTHER,
        "course_feature_tag": TAG_ONLINE_TEXTBOOK,
    },
    {
        "ocw_feature": FEATURE_ONLINE_TEXTBOOKS,
        "ocw_subfeature": SUBFEATURE_MATHML,
        "course_feature_tag": TAG_ONLINE_TEXTBOOK,
    },
    {
        "ocw_feature": FEATURE_ONLINE_TEXTBOOKS,
        "ocw_subfeature": SUBFEATURE_HTML,
        "course_feature_tag": TAG_ONLINE_TEXTBOOK,
    },
    {
        "ocw_feature": FEATURE_ONLINE_TEXTBOOKS,
        "ocw_subfeature": SUBFEATURE_PDF,
        "course_feature_tag": TAG_ONLINE_TEXTBOOK,
    },
    {
        "ocw_feature": FEATURE_PROJECTS,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_PROJECTS,
    },
    {
        "ocw_feature": FEATURE_PROJECTS,
        "ocw_subfeature": SUBFEATURE_EXAMPLES,
        "course_feature_tag": TAG_PROJECTS_WITH_EXAMPLES,
    },
    {
        "ocw_feature": FEATURE_PROJECTS,
        "ocw_subfeature": SUBFEATURE_NO_EXAMPLES,
        "course_feature_tag": TAG_PROJECTS,
    },
    {
        "ocw_feature": FEATURE_RESOURCE_INDEX,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_NONE,
    },
    {
        "ocw_feature": FEATURE_THIS_COURSE_AT_MIT,
        "ocw_subfeature": SUBFEATURE_NONE,
        "course_feature_tag": TAG_NONE,
    },
]


def match_course_feature_tag(feature, subfeature):
    """
    Matches a course feature tag to an ocw_feature / ocw_subfeature pair

    Args:
        feature (string): the ocw_feature property
        subfeature (string): the ocw_subfeature property

    Returns:
        string: the course_feature_tag that matches the passed in feature and subfeature
    """
    return next(
        (
            x["course_feature_tag"]
            for x in TAG_MAPPING
            if x["ocw_feature"] == feature and x["ocw_subfeature"] == subfeature
        ),
        None,
    )