const Validators = {
  list: {
    name: {
      required: {
        message: "Please enter a name",
        value: true,
      },
      maxLength: {
        message: "Name must be less than or equal to 50 characters",
        value: 50,
      },
      minLength: {
        message: "Name must be greater than or equal to 5 characters",
        value: 5,
      },
    },
    slug: {
      required: {
        message: "Please enter a slug, this will be used to identify your list",
        value: true,
      },
      maxLength: {
        message: "Slug must be less than or equal to 50 characters",
        value: 50,
      },
      minLength: {
        message: "Slug must be greater than or equal to 5 characters",
        value: 5,
      },
    },
    description: {
      maxLength: {
        message: "Description must be less than or equal to 255 characters",
        value: 255,
      },
    },
  },
};

export default Validators;