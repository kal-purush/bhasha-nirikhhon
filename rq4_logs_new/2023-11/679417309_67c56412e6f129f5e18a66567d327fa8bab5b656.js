module.exports = {
    type: 'object',
    properties: {
        name: {
          type: "string",
        },
        description: {
          type: "string",
        },
        image: {
          type: "string",
        },
        price: {
          type: "number",
        },
      },
      required: ["name", "description", "image", "price"],
      additionalProperties: false,
}