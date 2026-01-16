import { saveApplicantArguments } from "$models/MutationArguments";

describe("saveApplicantArguments", () => {
  it("returns the variables for the useSaveApplicant mutation", async () => {
    const inputVariables = {
      user: {
        email: "name@gmail.com",
        dni: "39488888",
        name: "name",
        surname: "surname",
        password: "password"
      },
      padron: 98544,
      careers: [{
        isGraduate: false,
        careerCode: "10",
        approvedSubjectCount: 35,
        currentCareerYear: 5
      }]
    };
    const variables = saveApplicantArguments({ ...inputVariables });
    expect(variables).toEqual(inputVariables);
  });

  it("sets to undefined approvedSubjectCount and currentCareerYear if isGraduate", async () => {
    const name = "name";
    const surname = "surname";
    const inputVariables = {
      user: {
        email: "name@gmail.com",
        dni: "39488888",
        name: "name",
        surname: "surname",
        password: "password"
      },
      padron: 98544,
      careers: [{
        isGraduate: true,
        careerCode: "10",
        approvedSubjectCount: 35,
        currentCareerYear: 5
      }]
    };
    const variables = saveApplicantArguments(inputVariables);
    expect(variables).toEqual({
      user: { name, surname },
      ...inputVariables,
      careers: [{
        isGraduate: true,
        careerCode: "10",
        approvedSubjectCount: undefined,
        currentCareerYear: undefined
      }]
    });
  });
});