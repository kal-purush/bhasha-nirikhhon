export const getQuestions = () => {
  fetch("https://opentdb.com/api.php?amount=15&type=multiple")
    .then((response) => response.json())
    .then((data) => {
      const question = data.results;
      console.log(question);
    });
};