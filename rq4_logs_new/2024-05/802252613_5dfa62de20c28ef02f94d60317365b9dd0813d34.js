let questionBody = document.querySelector("tbody")
const triviaQuestion = async () =>{
    try {
        const response = await fetch("https://opentdb.com/api.php?amount=30&category=23")
        const data = await response.json()
        const newData = data.results
        console.log(newData)
        // return data

        newData.map((item)=>{
            const newTask = document.createElement("tr")
            newTask.innerHTML =
            `
            <td>${item.question}</td>
            <td>${item.difficulty}</td>
            <td>${item.correct_answer}</td>
            `
            questionBody.appendChild(newTask)
        })
    } catch (error) {
        console.log("Error Found")
    }
}
triviaQuestion()