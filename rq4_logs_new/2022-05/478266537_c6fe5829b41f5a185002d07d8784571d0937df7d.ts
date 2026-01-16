import { Plan } from "../interfaces/plan";

export const samplePlan: Plan = {
    name: "1",
    years: [
        {
            name: "Fresh",
            semesters: [
                {
                    id: "Fresh1",
                    season: "Fall",
                    courses: [
                        {
                            code: "CISC 101",
                            name: "Principles of Computing",
                            descr: "Introduces students to the central ideas of computing and computer science including programs, algorithms, abstraction, the internet, and information systems. Instills ideas and practices of computational thinking and engages students in activities that show how computing and computer science change the world. Explores computing as a creative activity and empowers students to apply computational thinking to all disciplines including the arts, humanities, business, social and physical sciences, health, and entertainment.",
                            credits: " 3",
                            preReq: "",
                            restrict: "",
                            breadth:
                                "University: Mathematics, Natural Sciences and Technology; A&S: GROUP D: A&S Math, Nat Sci & Technology",
                            typ: "Fall, Winter and Spring"
                        }
                    ]
                },
                {
                    id: "Fresh2",
                    season: "Winter",
                    courses: [
                        {
                            code: "CISC 103",
                            name: "Introduction to Computer Science with Web Applications",
                            descr: "Principles of computer science illustrated through programming in scripting languages such as JavaScript and VBScript. Topics include control structures, arrays, functions, and procedures. Programming projects illustrate web-based applications.",
                            credits: " 3",
                            preReq: "",
                            restrict: "Open to non-majors.",
                            breadth:
                                "University: Mathematics, Natural Sciences and Technology; A&S: GROUP D: A&S Math, Nat Sci & Technology",
                            typ: "Fall and Spring"
                        }
                    ]
                },
                {
                    id: "Fresh3",
                    season: "Spring",
                    courses: [
                        {
                            code: "CISC 106",
                            name: "General Computer Science for Engineers",
                            descr: "Principles of computer science illustrated and applied through programming in a general-purpose language. Programming projects illustrate computational problems, styles, and issues that arise in engineering.",
                            credits: " 3",
                            preReq: "",
                            restrict: "",
                            breadth:
                                "University: Mathematics, Natural Sciences and Technology; A&S: GROUP D: A&S Math, Nat Sci & Technology",
                            typ: "Fall, Summer and Spring"
                        }
                    ]
                },
                {
                    id: "Fresh4",
                    season: "Summer",
                    courses: [
                        {
                            code: "CISC 108",
                            name: "Introduction to Computer Science I",
                            descr: "Computing and principles of programming with an emphasis on systematic program design. Topics include functional programming, data abstraction, procedural abstraction, use of control and state, recursion, testing, and object-oriented programming concepts. Requires no prior programming experience, open to any major, but intended primarily for majors and minors in computer science or mathematics.",
                            credits: " 3",
                            preReq: "",
                            restrict: "",
                            breadth:
                                "University: Mathematics, Natural Sciences and Technology; A&S: GROUP D: A&S Math, Nat Sci & Technology",
                            typ: "Fall and Spring"
                        }
                    ]
                }
            ]
        }
    ]
};