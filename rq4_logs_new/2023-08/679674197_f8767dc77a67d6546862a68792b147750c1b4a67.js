const btn = document.getElementById('options');
const sub = document.getElementById('subOptions');
const jokes = document.getElementById('dadJokes');
const myDiv = document.getElementById('result');
const options = {
  method: 'GET',
  headers: {
    'X-RapidAPI-Key': 'c2061c6ed8mshf5a80186e532224p12502fjsn4c1473b899c6',
    'X-RapidAPI-Host': 'dad-jokes.p.rapidapi.com'
  },
  mode: 'cors'
};
btn.addEventListener('change', async () => {
  jokes.setAttribute('disabled', 'disabled');
    const selected = btn.value; 
    console.log(selected)
    if (selected == 'random') {
      return dadJokes()
    }
    myDiv.innerHTML = '';
    console.log('button clicked')
      try {
          const result = await fetch(`https://tech-exams.p.rapidapi.com/${selected}`, options);
          const response = await result.json()
          const data = response.data;
          console.log(data)
          if (selected == 'AllExams') {
            AllExams(data)
          } else if (selected == 'QandA') {
            QandA(data)
          } 
          else {
            AllQuestions(data)
          }

      } catch (error) {
          console.error(error);
      }
      
})

sub.addEventListener('change', async () => {
  jokes.setAttribute('disabled', 'disabled');
    const subSelected = sub.value || '';
    myDiv.innerHTML = '';
    console.log('button clicked')
      try {
          const result = await fetch(`https://tech-exams.p.rapidapi.com/getQuestionsByExam/${subSelected}`, options);
          const response = await result.json()
          const data = response.data;
          console.log(data)
          if (data.length > 0) {
            getQuestionsByExam(data);
          } else {
            const h3 = document.createElement('h3');
            h3.innerHTML = "The exam questions are not available yet";
            myDiv.append(h3);
          }
      } catch (error) {
          console.error(error);
      }
})


const AllExams = (data) => {
    document.getElementById('subOptions').removeAttribute('disabled')
    jokes.setAttribute('disabled', 'disabled');
    const table = document.createElement('table');
    const tableHeaderRow = document.createElement('tr');
    const header1 = document.createElement('th');
    header1.textContent = "Exam Number";
    const header2 = document.createElement('th');
    header2.textContent = "Exam Provider";
    const header3 = document.createElement('th');
    header3.textContent = "Exam Description";
    
    tableHeaderRow.appendChild(header1);
    tableHeaderRow.appendChild(header2);
    tableHeaderRow.appendChild(header3);
    table.appendChild(tableHeaderRow);
    for (let i = 0; i < data.length; i++) {
        const rowData = data[i];
        const row = document.createElement('tr');
        
        const cell1 = document.createElement('td');
        cell1.textContent = rowData.examNumber;
        row.appendChild(cell1);
        
        const cell2 = document.createElement('td');
        cell2.textContent = rowData.examProvider;
        row.appendChild(cell2);
        
        const cell3 = document.createElement('td');
        cell3.textContent = rowData.examDescription;
        row.appendChild(cell3);
        
        table.appendChild(row);
    }
    myDiv.appendChild(table);
}


const QandA = (data) => {
  jokes.setAttribute('disabled', 'disabled');
    let question = 1
    document.getElementById('subOptions').setAttribute('disabled', 'disabled')
    for (let i = 0; i< data.length; i++) {
        const h3 = document.createElement('h3');
        const p = document.createElement('p');
        p.append(data[i].choiceDescription);
        h3.append(`${question}. ${data[i].questionDescription}`);
        myDiv.append(h3);
        myDiv.append(p);
        question++;
    }
}

const AllQuestions = (data) => {
  jokes.setAttribute('disabled', 'disabled');
    let questionId = 1;
    document.getElementById('subOptions').setAttribute('disabled', 'disabled')
    for (let i = 0; i< data.length; i++) {
        const h3 = document.createElement('h3');
        h3.append(`${questionId}) ${data[i].questionDescription}`);
        myDiv.append(h3)
        for(let j = 0; j< data[i].choices.length; j++) {
            const p = document.createElement('p');
            p.append(data[i].choices[j].choiceDescription);
            myDiv.append(p);
        }
        questionId++;
      }
}

const getQuestionsByExam = (data) => {
  jokes.setAttribute('disabled', 'disabled');
    let questionNo
    for (let i = 0; i< data.length; i++) {
        const h3 = document.createElement('h3');
        const p = document.createElement('p');
        p.append(data[i].choices[0].choiceDescription);
        h3.append(`${questionNo}. ${data[i].questionDescription}`);
        myDiv.append(h3);
        myDiv.append(p);
        questionNo++;
    }

}

const dadJokes = () => {
  jokes.removeAttribute('disabled')
  jokes.addEventListener('change', async() => {
      const selectedJoke = jokes.value;
      let baseUrl;
      console.log(selectedJoke)
      if (selectedJoke == 'joke') {
        baseUrl = `https://dad-jokes.p.rapidapi.com/random/${selectedJoke}`;
      } else {
        baseUrl = `https://dad-jokes.p.rapidapi.com/random/joke/${selectedJoke}`;
      }
        try {
          const result = await fetch(baseUrl, options)
          const response = await result.json();
          const data = response.body;
          if (selectedJoke == 'joke') {
            getRandomJoke(data);
          } else {
            getRandomJokeImage(data);
          }
        } catch (error) {
          
        }
  } )
}

const getRandomJoke = (data) => {
  console.log(data)
  alert(data[0].setup);
  alert(data[0].punchline);

}

const getRandomJokeImage = (data) => {
  console.log(data)
  myDiv.innerHTML = ''
  const img = document.createElement('img');
  img.setAttribute('src', data.image);
  myDiv.append(img)
}
