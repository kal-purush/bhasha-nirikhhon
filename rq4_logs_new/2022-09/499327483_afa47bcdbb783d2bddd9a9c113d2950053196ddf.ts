import { JsonSchema7 } from "@jsonforms/core";

const province = [
	"Chaguanas",
	"Couva",
	"Diego Martin",
	"Mayaro",
	"Penal",
	"Princes Town",
	"Point Fortin",
	"Port of Spain",
];

export const name = "Marriage certificate";
export const schema: JsonSchema7 = {
	type: "object",
	properties: {
		name: {
			type: "object",
			properties: {
				firstName: {
					type: "string",
					minLength: 3,
					description: "Please enter your first name",
					errorMessage: {
						type: "First name is required",
						minLength: "First name must be at least 3 characters",
					},
				},
				lastName: {
					type: "string",
					minLength: 3,
					description: "Please enter your last name",
					errorMessage: {
						type: "Last name is required",
						minLength: "Last name must be at least 3 characters",
					},
				},
			},
		},
		address: {
			type: "object",
			properties: {
				street: {
					type: "string",
					minLength: 3,
					description: "Please enter your street",
					errorMessage: {
						type: "Street is required",
						minLength: "Street name must be at least 3 characters",
					},
				},
				city: {
					type: "string",
					minLength: 3,
					description: "Please enter your city",
					errorMessage: {
						type: "City is required",
						minLength: "City name must be at least 3 characters",
					},
				},
				state: {
					type: "string",
					enum: province,
					description: "Please select a state or province",
					errorMessage: {
						type: "Please select a state or province",
					},
				},
			},
		},
		phoneNumber: { type: "string" },
		identification: {
			type: "object",
			properties: {
				type: {
					oneOf: [
						{ const: "ID", title: "ID" },
						{ const: "DP", title: "DP" },
						{ const: "PP", title: "PP" },
					],
				},
        number: {
          type: "string"
        }
			},
		},

		groom: {
			type: "object",
			properties: {
				firstName: {
					type: "string",
					minLength: 3,
					description: "Please enter your first name",
					errorMessage: {
						type: "First name is required",
						minLength: "First name must be at least 3 characters",
					},
				},
				lastName: {
					type: "string",
					minLength: 3,
					description: "Please enter your last name",
					errorMessage: {
						type: "Last name is required",
						minLength: "Last name must be at least 3 characters",
					},
				},
			},
		},
		bride: {
			type: "object",
			properties: {
				firstName: {
					type: "string",
					minLength: 3,
					description: "Please enter your first name",
					errorMessage: {
						type: "First name is required",
						minLength: "First name must be at least 3 characters",
					},
				},
				lastName: {
					type: "string",
					minLength: 3,
					description: "Please enter your last name",
					errorMessage: {
						type: "Last name is required",
						minLength: "Last name must be at least 3 characters",
					},
				},
			},
		},
		dateOfMarriage: {
			type: "string",
			format: "date",
		},
		placeOfMarriage: {
			type: "string",
		},
		typeOfMarriage: {
			type: "string",
			enum: ["Civil", "Hindu", "Muslim", "Orisha", "In Extremis"],
		},
	},
};
export const uischema = {
	type: "Categorization",
	elements: [
		{
			type: "Category",
			label: "Name of applicant",
			elements: [
				{
					type: "Control",
					label: "First name",
					scope: "#/properties/name/properties/firstName",
				},
				{
					type: "Control",
					label: "Last name",
					scope: "#/properties/name/properties/lastName",
				},
			],
		},
		{
			type: "Category",
			label: "Where do you currently live?",
			elements: [
				{
					type: "Control",
					label: "Street",
					scope: "#/properties/address/properties/street",
				},
				{
					type: "Control",
					label: "City",
					scope: "#/properties/address/properties/city",
				},
				{
					type: "Control",
					label: "State / Province",
					scope: "#/properties/address/properties/state",
				},
			],
		},
		{
			type: "Category",
			label: "What number can we use to reach you?",
			elements: [
				{
					type: "Control",
					label: "Enter your phone number",
					scope: "#/properties/phoneNumber",
				},
			],
		},
		{
			type: "Category",
			label: "Type of identification",
			elements: [
				{
					type: "Control",
					scope: "#/properties/identification/properties/type",
          options: { format: "radio" },
				},
				{
					type: "Control",
					scope: "#/properties/identification/properties/number"
				},
			],
		},
		{
			type: "Category",
			label: "Groom's full name",
			elements: [
				{
					type: "Control",
					label: "First name",
					scope: "#/properties/groom/properties/firstName",
				},
				{
					type: "Control",
					label: "Last name",
					scope: "#/properties/groom/properties/lastName",
				},
			],
		},
		{
			type: "Category",
			label: "Bride's full name",
			elements: [
				{
					type: "Control",
					label: "First name",
					scope: "#/properties/bride/properties/firstName",
				},
				{
					type: "Control",
					label: "Last name",
					scope: "#/properties/bride/properties/lastName",
				},
			],
		},
		{
			type: "Category",
			label: "Date of marriage",
			elements: [
				{
					type: "Control",
					scope: "#/properties/dateOfMarriage",
				},
			],
		},
		{
			type: "Category",
			label: "Place of marriage",
			elements: [
				{
					type: "Control",
					scope: "#/properties/placeOfMarriage",
				},
			],
		},
		{
			type: "Category",
			label: "Type of marriage",
			elements: [
				{
					type: "Control",
					scope: "#/properties/typeOfMarriage",
				},
			],
		},
	],
	options: { variant: "stepper" },
};