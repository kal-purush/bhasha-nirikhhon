import { Project, Role, Type } from '../../types/Interface';

const exampleProject: Project = {
	projectName: '',
	projectId: '',
	estimatedBy: '',
	estimationDate: '',
	verifiedBy: '',
	verificationDate: '',
	estStart: '',
	estEns: '',
	teamSize: 0,
	timeBudget: 0,
	effort: 0,
	sections: [
		{
			sectionId: Role.FD,
			name: 'Frontend development',
			minMd: 19.25,
			minMdFormula: '',
			maxMd: 56.25,
			maxMdFormula: '',
			predictedMd: 46.25,
			predictedMdFormula: '',
			risk: 21.62,
			riskFormula: '',
			tasks: [
				{
					id: "123",
					type: Type.Task,
					name: "Business feature",
					role: Role.FD,
					minMd: 10,
					maxMd: 30,
					predictedMd: 25,
					predictedMdFormula: "",
					risk: "H",
					comment: {
						text: "",                            
						isImportant: false
					},
					subtasks: [
						{
							id: "abc",
							type: Type.Task,
							name: "s1",
							role: Role.FD,
							minMd: 10,
							maxMd: 30,
							predictedMd: 25,
							predictedMdFormula: "",
							risk: "H",
							comment: {
								text: "Business feature is better described as doing something i.e. logging into a system, printing a report or editing a text",                            
								isImportant: true
							},
							subtasks: []
						},
						{
							id: "def",
							type: Type.Task,
							name: "s2",
							role: Role.FD,
							minMd: 10,
							maxMd: 30,
							predictedMd: 25,
							predictedMdFormula: "",
							risk: "H",
							comment: {
								text: "Business feature is better described as doing something i.e. logging into a system, printing a report or editing a text",                            
								isImportant: false
							},
							subtasks: []
						}
					]
				},
				{
					id: "678",
					type: Type.Task,
					name: "Business feature 3",
					role: Role.FD,
					minMd: 10,
					maxMd: 30,
					predictedMd: 25,
					predictedMdFormula: "",
					risk: "L",
					comment: {
						text: "",                            
						isImportant: false
					},
					subtasks: []
				},
				{
					id: "879",
					type: Type.Task,
					name: "Business feature 4",
					role: Role.FD,
					minMd: 10,
					maxMd: 30,
					predictedMd: 25,
					predictedMdFormula: "",
					risk: "M",
					comment: {
						text: "Business feature is better described as doing something i.e. logging into a system, printing a report or editing a text",                            
						isImportant: false
					},
					subtasks: []
				}				
			]
		},
		{
			sectionId: Role.BD,
			name: 'Backend development',
			minMd: 0,
			minMdFormula: '',
			maxMd: 0,
			maxMdFormula: '',
			predictedMd: 0,
			predictedMdFormula: '',
			risk: 0,
			riskFormula: '',
			tasks: [],
		},
		{
			sectionId: Role.MD,
			name: 'Mobile development',
			minMd: 0,
			minMdFormula: '',
			maxMd: 0,
			maxMdFormula: '',
			predictedMd: 0,
			predictedMdFormula: '',
			risk: 0,
			riskFormula: '',
			tasks: [],
		},
		{
			sectionId: Role.UD,
			name: 'Design / UX / UI',
			minMd: 0,
			minMdFormula: '',
			maxMd: 0,
			maxMdFormula: '',
			predictedMd: 0,
			predictedMdFormula: '',
			risk: 0,
			riskFormula: '',
			tasks: [],
		},
		{
			sectionId: Role.DO,
			name: 'Configuration / Setup / Deployment',
			minMd: 0,
			minMdFormula: '',
			maxMd: 0,
			maxMdFormula: '',
			predictedMd: 0,
			predictedMdFormula: '',
			risk: 0,
			riskFormula: '',
			tasks: [],
		},
	],
	rawDevelopmentEffortSum: {
		name: '',
		main: {},
		parts: [],
	},
	summary: [],
	assumptions: [],
};

export default exampleProject;