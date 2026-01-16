import { JsonSchema7 } from "@jsonforms/core";

export const createUsernameSchema: JsonSchema7 = {
	$id: "createUsername",
	type: "object",
	properties: {
		username: {
			type: "string",
			minLength: 3,
			maxLength: 15,
			description: "Please enter a username",
			errorMessage: {
				type: "Please enter a username",
				minLength: "Username must be at least 3 characters long",
				maxLength: "Username must be at most 15 characters long",
			},
		},
	}
};

export const createUsernameUischema = {
  type: "VerticalLayout",
  elements: [
    {
      type: "Control",
      label: "Username",
      scope: "#/properties/username",
    }
  ]
};