import * as yup from "yup";

const urlValidationTemp = "URL must be a valid URL";

export const schema = yup.object().shape({
  bio: yup.string(),
  location: yup.string(),
  githubUrl: yup.string().url(`Github ${urlValidationTemp}`),
  twitterUrl: yup.string().url(`Twitter ${urlValidationTemp}`),
  mediumUrl: yup.string().url(`Medium ${urlValidationTemp}`),
  stackOverflowUrl: yup.string().url(`Stack Overflow ${urlValidationTemp}`),
  websiteUrl: yup.string().url(`Website ${urlValidationTemp}`),
});