import { SurveyQuestion } from "./types";

export const defaultQuestions: SurveyQuestion[] = [
	{
		question: {
			hash: "identity",
			description: "What do you identify as?",
		},
		options: [
			{
				hash: "jadvii",
				description: "American Indian/Alaskan Native",
			},
			{
				hash: "f4nyz7",
				description: "Asian",
			},
			{
				hash: "lzrsqj",
				description: "Black or African American",
			},
			{
				hash: "fv0yuu",
				description: "Hispanic/Latino",
			},
			{
				hash: "gz2wfq",
				description: "Native Hawaiian/Other Pacific Islander",
			},
			{
				hash: "xbelox",
				description: "White",
			},
			{
				hash: "pe49er",
				description: "Other",
			},
		],
	},
	{
		question: {
			hash: "pronoun",
			description: "What are your pronouns?",
		},
		options: [
			{
				hash: "aw1j1l",
				description: "She/Her",
			},
			{
				hash: "gl21j2",
				description: "He/Him",
			},
			{
				hash: "rbymcm",
				description: "They/them",
			},
			{
				hash: "012aqt",
				description: "I prefer not to say",
			},
		],
	},
	{
		question: {
			hash: "college",
			description: "What college are you in?",
		},
		options: [
			{
				hash: "ll123j",
				description: "Architecture, Art and Planning",
			},
			{
				hash: "rlk121",
				description: "Arts & Sciences",
			},
			{
				hash: "arw6k0",
				description: "Agriculture & Life Sciences",
			},
			{
				hash: "vnw299",
				description: "Arts & Sciences",
			},
			{
				hash: "w1o200",
				description: "Human Ecology",
			},
			{
				hash: "apbv19",
				description: "SC Johnson College of Business",
			},
			{
				hash: "e12po0",
				description: "Engineering",
			},
		],
	},
	{
		question: {
			hash: "location",
			description: "Are you studying remotely this semester?",
		},
		options: [
			{
				hash: "wzb22o",
				description: "No, I'm on campus",
			},
			{
				hash: "bapn22",
				description: "Yes, I’m outside of Ithaca, but still in the US",
			},
			{
				hash: "wpoen",
				description: "Yes, I’m in a country outside the US",
			},
		],
	},
	{
		question: {
			hash: "mode",
			description: "How do you prefer studying with groups this year?",
		},
		options: [
			{
				hash: "nnwpo2",
				description: "Online",
			},
			{
				hash: "19wnbe",
				description: "In-person",
			},
			{
				hash: "vbpo2b",
				description: "both",
			},
		],
	},
	{
		question: {
			hash: "time",
			description: "When do you prefer studying for this course?",
		},
		options: [
			{
				hash: "anaw2k",
				description: "Morning (8am-12pm)",
			},
			{
				hash: "ae12fb",
				description: "Early Afternoon (12pm-4pm)",
			},
			{
				hash: "xfn2j3",
				description: "Late Afternoon (4pm-8pm)",
			},
			{
				hash: "apb112",
				description: "Evening (8pm-12am)",
			},
			{
				hash: "bpwon2",
				description: "Late (12am-4am)",
			},
		],
	},
	{
		question: {
			hash: "start",
			description: "You have an assignment. When do you start it?",
		},
		options: [
			{
				hash: "ssen2o",
				description: "Immediately, the day you get it",
			},
			{
				hash: "9fbbvd",
				description: "Early, but not immediately",
			},
			{
				hash: "bnzml2",
				description: "Midway",
			},
			{
				hash: "bb23rb",
				description: "Late, the day before",
			},
			{
				hash: "aln11b",
				description: "I don’t / I submit late",
			},
		],
	},
	{
		question: {
			hash: "graduation",
			description: "When are you graduating?",
		},
		options: [
			{
				hash: "bp3on9",
				description: "2022",
			},
			{
				hash: "eobb31",
				description: "2023",
			},
			{
				hash: "iie81g",
				description: "2024",
			},
			{
				hash: "2bf9rn",
				description: "2025",
			},
			{
				hash: "urywe8",
				description: "2026",
			},
		],
	},
];