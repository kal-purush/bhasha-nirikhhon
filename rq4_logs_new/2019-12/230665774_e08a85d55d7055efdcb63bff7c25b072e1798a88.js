import React from "react";
import { mount, configure } from "enzyme";
import Adapter from "enzyme-adapter-react-16";
configure({ adapter: new Adapter() });

import StartQuizContainer from "../../../../screens/startQuiz";

describe("When our app loads for the first time", () => {
  const quizData = require("../quizData.json");
  jest.spyOn(window, "alert").mockImplementation(() => {});
  window.alert = jest.fn();

  const wrapper = mount(<StartQuizContainer />);

  it("Should show a welcome screen with start quiz button", () => {
    const startQuizButton = wrapper.find("[data-testid='start-quiz-button']");
    expect(startQuizButton.exists()).toBe(true);
  });

  it("Should show the quiz question screen when clicks on startQuiz button", () => {
    const startQuizButton = wrapper
      .find("[data-testid='start-quiz-button']")
      .last();

    startQuizButton.simulate("click");

    const quizContainerWrapper = wrapper.find("[data-testid='quiz-container']");
    expect(quizContainerWrapper.exists()).toBe(true);
  });

  it("Should show a questionNav button bar with next and previous buttons", () => {
    const questionButtonNavBarWrapper = wrapper.find(
      "[data-testid='question-nav-bar-container']"
    );
    expect(questionButtonNavBarWrapper.exists()).toBe(true);
  });

  it("Should show a question with 4 options", () => {
    const expectedQuestion = quizData[0].question;
    const expectedOptions = quizData[0].options;
    const questionWrapper = wrapper.find("Card");
    expect(questionWrapper.exists()).toBe(true);
    const questionDisplayed = wrapper.find("CardHeader").text();
    expect(questionDisplayed).toBe(expectedQuestion);
    const optionsDisplayed = wrapper
      .find("label")
      .map(eachOption => eachOption.text());
    expect(optionsDisplayed.length).toBe(4);
    expect(optionsDisplayed).toEqual(expectedOptions);
  });

  it("Should show cancel and submit quiz sbuttons", () => {
    const submitQuizButtonWrapper = wrapper.find(
      "[data-testid='submit-quiz-button']"
    );
    const cancelButtonWrapper = wrapper.find("[data-testid='cancel-button']");
    expect(submitQuizButtonWrapper.exists()).toBe(true);
    expect(cancelButtonWrapper.exists()).toBe(true);
  });

  it("Should able to select option ", () => {
    wrapper.find("input").forEach((eachOption, index) => {
      if (index === 1)
        eachOption.simulate("change", { target: { checked: true } });
    });
    expect(wrapper.state().optionsSelected).toEqual({ question1: "Component" });
  });

  it("Should able to jump to a question using button nav bar", () => {
    const navButtonWrapper = wrapper
      .find("[data-testid='nav-Buttpon-3']")
      .last();
    navButtonWrapper.simulate("click");
    expect(wrapper.state().selectedQuestion).toBe(3);
  });

  it("Should show a pop up of quiz result when clicks on the submit quiz button", () => {
    const submitQuizButtonWrapper = wrapper
      .find("[data-testid='submit-quiz-button']")
      .last();
    submitQuizButtonWrapper.simulate("click");
    expect(alert).toBeCalled();
    expect(alert).toHaveBeenCalledTimes(1);
    expect(alert).toHaveBeenCalledWith(
      "Your Quiz result is :- Right answers:1 & Wrong answers: 4"
    );
  });
});