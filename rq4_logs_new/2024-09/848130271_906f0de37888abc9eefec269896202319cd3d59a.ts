export const getUserSchema = {
  params: {
    type: "object",
    required: ["id"],
    properties: {
      id: { type: "string", description: "User ID" },
    },
  },
};

export const addUserSchema = {
  body: {
    type: "object",
    required: ["name", "age"],
    properties: {
      name: { type: "string"},
      age: { type: "integer" },
    },
  },
};

export const updateUserSchema = {
  params: {
    type: "object",
    required: ["id"],
    properties: {
      id: { type: "string", description: "User ID" },
    },
  },
  body: {
    type: "object",
    properties: {
      name: { type: "string", description: "Name of the user" },
      age: { type: "integer", description: "Age of the user" },
    },
  },
};

export const deleteUserSchema = {
  params: {
    type: "object",
    required: ["id"],
    properties: {
      id: { type: "string", description: "User ID" },
    },
  },
};