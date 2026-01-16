const startButton = document.getElementById('start-btn')
const nextButton = document.getElementById('next-btn')
const questionContainerElement = document.getElementById('question-container')
const questionElement = document.getElementById('question')
const answerButtonsElement = document.getElementById('answer-buttons')
const imgSrc = document.getElementById('country')
const ingredList = document.getElementById('ingredients')
const explain = document.getElementById('explanation-container')
const splashP = document.getElementById('splash')

let shuffledQuestions, currentQuestionIndex

startButton.addEventListener('click', startGame)
startButton.addEventListener('click', splashPage)
nextButton.addEventListener('click', () => {
  currentQuestionIndex++
  setNextQuestion()
})

function startGame() {
  startButton.classList.add('hide')
  shuffledQuestions = questions.sort(() => Math.random() - .5)
  currentQuestionIndex = 0
  questionContainerElement.classList.remove('hide')
  setNextQuestion()
  showImage(shuffledQuestions[currentQuestionIndex])
  showIngredients(shuffledQuestions[currentQuestionIndex])
  resetExplanation()
}

function setNextQuestion() {
  resetState()
  showQuestion(shuffledQuestions[currentQuestionIndex])
  showImage(shuffledQuestions[currentQuestionIndex])
  showIngredients(shuffledQuestions[currentQuestionIndex])
  
}

function showQuestion(question) {
    function explainCorrect() {
        explain.classList.remove('hide')
        explain.innerHTML = '<b>Correct!&nbsp;</b>' + question.explanation;
        explain.classList.add('explanation-correct')
    }
    function explainWrong() {
        explain.classList.remove('hide')
        explain.innerHTML = '<b>Incorrect!&nbsp;</b>' + question.explanation;
        explain.classList.add('explanation-wrong')
    }
    questionElement.innerText = question.question
    question.answers.forEach(answer => {
    const button = document.createElement('button')
    button.innerText = answer.text
    button.classList.add('btn')
    if (answer.correct) {
        button.dataset.correct = answer.correct;
        button.addEventListener('click', explainCorrect)
    }
    else {
        button.addEventListener('click', explainWrong)
    }
    button.addEventListener('click', selectAnswer)
    answerButtonsElement.appendChild(button)
  })
}

function showImage(question) {
    imgSrc.innerHTML = "<img src="+ question.imgSrc +">";
}

function showIngredients(question) {
    ingredList.innerHTML = "<h2>Ingredients</h2>"+ question.ingredList +"</p>";
}

function splashPage() {
    splashP.classList.add('hide')
}

function resetExplanation() {
    explain.classList.add('hide')
    explain.classList.remove('explanation-wrong')
    explain.classList.remove('explanation-correct')
}

function resetState() {
  clearStatusClass(document.body)
  nextButton.classList.add('hide')
  nextButton.addEventListener('click', resetExplanation)
  while (answerButtonsElement.firstChild) {
    answerButtonsElement.removeChild(answerButtonsElement.firstChild)
  }
}

function selectAnswer(e) {
  const selectedButton = e.target
  const correct = selectedButton.dataset.correct
  setStatusClass(document.body, correct)
  Array.from(answerButtonsElement.children).forEach(button => {
    setStatusClass(button, button.dataset.correct)
  })
  if (shuffledQuestions.length > currentQuestionIndex + 1) {
    nextButton.classList.remove('hide')
  } else {
    startButton.innerText = 'Play again!'
    startButton.classList.remove('hide')
  }
}

function setStatusClass(element, correct) {
  clearStatusClass(element)
  if (correct) {
    element.classList.add('correct')
  } else {
    element.classList.add('wrong')
  }
}

function clearStatusClass(element) {
  element.classList.remove('correct')
  element.classList.remove('wrong')
}


const questions = [
  {
    question: "Where is this dish from?",
    imgSrc: "file:///Users/Hannah/Documents/quiz/img/153.jpeg",
    explanation: "This is the national dish of <b>Senegal</b>, called <i>ceebu jen</i> (sometimes spelled <i>thiebou dienne</i>), which means <i>rice and fish</i> in Wolof. It's also popular in neighboring countries such as Mauritania and the Gambia.",
    ingredList: "rice<br>fish<br>carrots<br>cabbage<br>cassava<br>eggplant<br>tomatoes<br>garlic<br>parsley<br>stock<br>",
    answers: [
      { text: 'Senegal', correct: true },
      { text: 'India', correct: false },
      { text: 'Namibia', correct: false },
      { text: 'Venezuela', correct: false },
      { text: 'Congo', correct: false },
      { text: 'Bhutan', correct: false },
      { text: 'Samoa', correct: false },
      { text: 'Kosovo', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "file:///Users/Hannah/Documents/quiz/img/139.jpeg",
    explanation: "This dessert, <i>halo halo</i>, is often served as street food in the <b>Philippines</b>!",
    ingredList: "shaved ice<br>ice cream<br>evaporated milk<br>jackfruit<br>coconut<br>sweetened beans<br>plantains<br>yam jelly<br>leche flan",
    answers: [
      { text: 'Israel', correct: false },
      { text: 'Sri Lanka', correct: false },
      { text: 'Thailand', correct: false },
      { text: 'Indonesia', correct: false },
      { text: 'Philippines', correct: true },
      { text: 'Tunisia', correct: false },
      { text: 'South Korea', correct: false },
      { text: 'Panama', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "file:///Users/Hannah/Documents/quiz/img/130.jpeg",
    explanation: "This is <i>fufu and egusi soup</i>, popular in West Africa and often eaten in <b>Nigeria.</b> Cassava or beans are mashed into a paste and cooked to make <i>fufu</i>. The <i>fufu</i> is used like bread to eat soup or stew. <i>Egusi</i> is a type of melon grown in Nigeria and neighboring countries. The seeds are dried and ground to make this meal.",
    ingredList: "cassava or beans<br>tomatoes<br>peppers<br>onion<br>garlic<br>palm oil<br>choice of meat<br>dried/smoked fish<br>bouillon<br>melon seeds<br>green leafy vegetable",
    answers: [
      { text: 'Botswana', correct: false },
      { text: 'Mongolia', correct: false },
      { text: 'Guatemala', correct: false },
      { text: 'Jamaica', correct: false },
      { text: 'Chad', correct: false },
      { text: 'Nigeria', correct: true },
      { text: 'Cambodia', correct: false },
      { text: 'Mauritania', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "file:///Users/Hannah/Documents/quiz/img/91.jpeg",
    explanation: "This is <i>tteokbokki</i> (떡볶이), a popular street food in <b>South Korea</b>!",
    ingredList: "rice cakes<br>fish cakes<br>green onion<br>eggs<br>chili paste<br>sugar<br>",
    answers: [
      { text: 'Japan', correct: false },
      { text: 'Venezuela', correct: false },
      { text: 'Mauritius', correct: false },
      { text: 'Kenya', correct: false },
      { text: 'S. Korea', correct: true },
      { text: 'Laos', correct: false },
      { text: 'Vietnam', correct: false },
      { text: 'Pakistan', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/1.jpeg",
    explanation: "<i>Kabuli palau</i> is the national dish of <b>Afghanistan</b>. It gets its name from the Afghan capital, Kabul. ",
    ingredList: "chicken or mutton<br>basmati rice<br>onions<br>carrots<br>raisins<br>almonds<br>butter<br>cumin<br>cardamom",
    answers: [
      { text: 'Serbia', correct: false },
      { text: 'Tunisia', correct: false },
      { text: 'Afghanistan', correct: true },
      { text: 'Lebanon', correct: false },
      { text: 'Malawi', correct: false },
      { text: 'Japan', correct: false },
      { text: 'Sri Lanka', correct: false },
      { text: 'The Gambia', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/4.jpeg",
    explanation: "<i>Cargols a la lluna</i> — land snails — are a popular dish in <b>Andorra</b> and the neighboring Catalonia region of Spain. They are often served with aioli or vinaigrette.",
    ingredList: "snails<br>oil<br>aioli",
    answers: [
      { text: 'Cuba', correct: false },
      { text: 'El Salvador', correct: false },
      { text: 'Gabon', correct: false },
      { text: 'Albania', correct: false },
      { text: 'China', correct: false },
      { text: 'Indonesia', correct: false },
      { text: 'Andorra', correct: true },
      { text: 'Finland', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/9.jpeg",
    explanation: "<b>Australia</b>'s finest <i>vegemite</i> is often enjoyed on toast or on a sandwich!",
    ingredList: "yeast extract<br>salt<br>malt extract<br>spices",
    answers: [
      { text: 'Singapore', correct: false },
      { text: 'Norway', correct: false },
      { text: 'Australia', correct: true },
      { text: 'Mozambique', correct: false },
      { text: 'Laos', correct: false },
      { text: 'Philippines', correct: false },
      { text: 'Colombia', correct: false },
      { text: 'Japan', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/12.jpeg",
    explanation: "This dish is <i>conch salad</i> from the Bahamas. Also popular is <i>cracked conch</i> or dee-fried conch meat. If you don't know, a conch is that big shell that you put next to your ear to hear the ocean.",
    ingredList: "conch<br>tomatoes<br>peppers<br>onions",
    answers: [
      { text: 'Japan', correct: false },
      { text: 'Vietnam', correct: false },
      { text: 'Sri Lanka', correct: false },
      { text: 'Oman', correct: false },
      { text: 'Bahamas', correct: true },
      { text: 'Guatemala', correct: false },
      { text: 'Ecuador', correct: false },
      { text: 'Brazil', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/15.jpeg",
    explanation: "This is a traditional dish made in Barbados, called <i>cou cou</i>. It's made with corn meal, okra, butter, and water. It's traditionally served with steamed <i>flying fish</i> with tomato-based gravy.",
    ingredList: "fish<br>corn meal<br>okra<br>onion<br>butter<br>garlic<br>sweet pepper<br>tomatoes<br>curry<br>thyme<br>parsley<br>pepper",
    answers: [
      { text: 'Cyprus', correct: false },
      { text: 'Malaysia', correct: false },
      { text: 'Thailand', correct: false },
      { text: 'USA', correct: false },
      { text: 'Guyana', correct: false },
      { text: 'Ghana', correct: false },
      { text: 'Italy', correct: false },
      { text: 'Barbados', correct: true }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/22.jpeg",
    explanation: "This is a form of dolma called <i>soğan dolması</i> in <b>Bosnia and Herzegovina</b>. It's made of onions stuffed with beef.",
    ingredList: "onions<br>minced beef<br>rice<br>oil<br>tomatoes<br>paprika<br>vinegar<br>yogurt<br>lemon",
    answers: [
      { text: 'Bosnia', correct: true },
      { text: 'Libya', correct: false },
      { text: 'France', correct: false },
      { text: 'Argentina', correct: false },
      { text: 'New Zealand', correct: true },
      { text: 'India', correct: false },
      { text: 'Iceland', correct: false },
      { text: 'Saudi Arabia', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/23.jpeg",
    explanation: "<i>Seswaa</i> is stewed meat that is often cooked in a large pot over a fire for many hours in <b>Botswana</b>. It is served during ceremonies and is often paired with a cornmeal porridge called <i>pap</i> and green leafy vegetables.",
    ingredList: "beef shoulder<br>salt<br>pepper<br>water<br>corn meal<br>butter<br>morogo",
    answers: [
      { text: 'Morocco', correct: false },
      { text: 'Botswana', correct: true },
      { text: 'Mongolia', correct: false },
      { text: 'Russia', correct: false },
      { text: 'Ireland', correct: false },
      { text: 'Panama', correct: false },
      { text: 'Mauritius', correct: false },
      { text: 'Australia', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/24.jpeg",
    explanation: "This is <i>pão de queijo</i>, cheese bread from <b>Brazil</b>!",
    ingredList: "tapioca flour<br>milk<br>parmesan cheese<br>mozzarella cheese<br>eggs<br>salt<br>water<br>oil",
    answers: [
      { text: 'Iran', correct: false },
      { text: 'Paraguay', correct: false },
      { text: 'UAE', correct: false },
      { text: 'Zambia', correct: false },
      { text: 'Brazil', correct: true },
      { text: 'Korea', correct: false },
      { text: 'New Zealand', correct: false },
      { text: 'Somalia', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/24-2.jpeg",
    explanation: "Yum! These are <i>brigadeiros</i> from <b>Brazil</b>!",
    ingredList: "chocolate<br>butter<br>sweetened condensed milk<br>sprinkles or nuts",
    answers: [
      { text: 'Turkey', correct: false },
      { text: 'France', correct: false },
      { text: 'Uzbekistan', correct: false },
      { text: 'Sweden', correct: false },
      { text: 'Mexico', correct: false },
      { text: 'Brazil', correct: true },
      { text: 'Canada', correct: false },
      { text: 'Laos', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/31.jpeg",
    explanation: "<i>Poutine</i> is a dish of fries and cheese curds, covered in gravy. In Quebec, <i>poutine</i> is French Canadian slang for <i>mess</i>.",
    ingredList: "potatoes<br>cheese curds<br>beef broth<br>flour<br>cornstarch<br>butter<br>salt<br>pepper",
    answers: [
      { text: 'Australia', correct: false },
      { text: 'Latvia', correct: false },
      { text: 'South Africa', correct: false },
      { text: 'Hungary', correct: false },
      { text: 'Spain', correct: false },
      { text: 'Syria', correct: false },
      { text: 'Canada', correct: true },
      { text: 'Nigeria', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/36.jpeg",
    explanation: "<i>Tang yuan</i> (汤圆) is a popular dessert in <b>China</b>, often eaten during Spring Festival.",
    ingredList: "glutinous rice<br>water<br>sesame seeds<br>sugar<br>butter<br>fresh ginger",
    answers: [
      { text: 'Korea', correct: false },
      { text: 'Suriname', correct: false },
      { text: 'Dominica', correct: false },
      { text: 'China', correct: true },
      { text: 'Japan', correct: false },
      { text: 'Thailand', correct: false },
      { text: 'USA', correct: false },
      { text: 'India', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/46.jpeg",
    explanation: "A traditional food from <b>Denmark</b>, <i>stegt flæsk</i> is grilled or fried pork belly served with potatoes and a creamy parsley sauce.",
    ingredList: "pork belly<br>potatoes<br>parsley<br>cream<br>butter<br>flour<br>lemon<br>nutmeg",
    answers: [
      { text: 'Sweden', correct: false },
      { text: 'Denmark', correct: true },
      { text: 'Poland', correct: false },
      { text: 'Tunisia', correct: false },
      { text: 'Namibia', correct: false },
      { text: 'Pakistan', correct: false },
      { text: 'Indonesia', correct: false },
      { text: 'Estonia', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/57.jpeg",
    explanation: "<i>Injera</i> is a popular bread in <b>Ethiopia</b> and neighboring countries. </i>Doro wat</i>, a chicken stew, is often served with it.",
    ingredList: "teff flour<br>yeast<br>chicken<br>onions<br>garlic<br>ginger<br>spices<br>cardamom<br>chiles<br>lime<br>butter<br>salt",
    answers: [
      { text: 'Netherlands', correct: false },
      { text: 'Lebanon', correct: false },
      { text: 'UAE', correct: false },
      { text: 'Nepal', correct: false },
      { text: 'Malaysia', correct: false },
      { text: 'Lesotho', correct: false },
      { text: 'Ethiopia', correct: true },
      { text: 'Papua New Guinea', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/71.jpeg",
    explanation: "This is <b>Guyana</b>'s national dish, <i>pepperpot</i>. It is often served at special events. The black color comes from cassareep, a thick black liquid made from cassava. Pork or beef feet give the stew a gooey texture. ",
    ingredList: "pork or beef<br>pig or cow feet<br>ox tail<br>cassava<br>hot peppers<br>cinnamon<br>clove<br>garlic<br>sugar<br>spices<br>salt",
    answers: [
      { text: 'Guyana', correct: true },
      { text: 'Korea', correct: false },
      { text: 'Russia', correct: false },
      { text: 'Finland', correct: false },
      { text: 'Cambodia', correct: false },
      { text: 'Honduras', correct: false },
      { text: 'Kenya', correct: false },
      { text: 'Peru', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/79.jpeg",
    explanation: "The <i>L.A. Times</i> called <i>masgouf<i> 'The dish that brought down a dictator' for its role in possibly giving away the whereabouts of Iraqi dictator Saddam Hussein. It's a national dish in danger becase of poor water quality and war, but it remains a favorite of people in <b>Iraq</b>, and its history goes back to pre-Biblical times. ",
    ingredList: "carp<br>tomatoes<br>tamarind<br>onions<br>lemon<br>oil<br>cloves<br>curry<br>dried limes<br>parsley",
    answers: [
      { text: 'Yemen', correct: false },
      { text: 'Greece', correct: false },
      { text: 'New Zealand', correct: false },
      { text: 'Israel', correct: false },
      { text: 'Sierra Leone', correct: false },
      { text: 'Denmark', correct: false },
      { text: 'Cyprus', correct: false },
      { text: 'Iraq', correct: true }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/109.jpeg",
    explanation: "<i>Tiguadege na</i> is a dish originally enjoyed by the Mandinka people of inland West Africa. It's the national dish of <b>Mali</b>. Similar versions (also called <i>mafé</i>) can be found in neighboring countries.",
    ingredList: "meat<br>ground peanuts<br>onions<br>tomatoes<br>garlic<br>carrots<br>potatoes<br>eggplant",
    answers: [
      { text: 'Costa Rica', correct: false },
      { text: 'Croatia', correct: false },
      { text: 'Botswana', correct: false },
      { text: 'Saudi Arabia', correct: false },
      { text: 'Mali', correct: true },
      { text: 'India', correct: false },
      { text: 'Myanmar', correct: false },
      { text: 'Eritrea', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/114.jpeg",
    explanation: "<i>Mole</i> was once considered food for peasants, but is now a celebrated national dish in <b>Mexico</b>. There are different versions, some made with chocolate, others with seeds, pineapples, or peanuts.",
    ingredList: "chilis<br>garlic<br>pork<br>chocolate<br>tomatoes<br>almonds<br>stock<br>bread<br>thyme<br>oregano<br>pepperleaf<br>rice<br>",
    answers: [
      { text: 'Japan', correct: false },
      { text: 'Mexico', correct: true },
      { text: 'Rwanda', correct: false },
      { text: 'Ireland', correct: false },
      { text: 'Portugal', correct: false },
      { text: 'Uruguay', correct: false },
      { text: 'Niger', correct: false },
      { text: 'Romania', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/120.jpeg",
    explanation: "<i>Moroccan couscous</i> is so representative of <b>Morocco</b> that its English name has the country name in it. It's often made with chicken and a variety of vegetables.",
    ingredList: "couscous<br>meat<br>carrots<br>chickpeas<br>potatoes<br>yams<br>tomatoes<br>onions<br>saffron<br>ginger",
    answers: [
      { text: 'Bolivia', correct: false },
      { text: 'Slovakia', correct: false },
      { text: 'Sudan', correct: false },
      { text: 'Finland', correct: false },
      { text: 'Haiti', correct: false },
      { text: 'Israel', correct: false },
      { text: 'Morocco', correct: true },
      { text: 'Egypt', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/122.jpeg",
    explanation: "This dish is by far the favorite of the creator of this game! In English it's called <i>pickled tea leaf salad</i> in English, or <i>lahpet thoke</i> in Burmese (လက်ဖက်သုတ်). It's from <b>Myanmar (Burma)</b>.",
    ingredList: "fermented tea leaves<br>tomatoes<br>cabbage<br>roasted nuts & seeds<br>sesame seeds<br>ginger<br>garlic<br>oil<br>fish sauce",
    answers: [
      { text: 'Algeria', correct: false },
      { text: 'Malaysia', correct: false },
      { text: 'Pakistan', correct: false },
      { text: 'Iraq', correct: false },
      { text: 'Burma', correct: true },
      { text: 'Costa Rica', correct: false },
      { text: 'Venezuela', correct: false },
      { text: 'Tajikistan', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/128.jpeg",
    explanation: "<i>Nacatama</i> is a dish from <b>Nicaragua</b>. It's made similar to Mexican tamales, but has different ingredients, such as olives, capers, and sour orange juice.",
    ingredList: "masa harina<br>lard<br>chicken broth<br>pork<br>sour orange juice<br>rice<br>potatoes<br>onion<br>peppers<br>tomatoes<br>olives<br>raisins<br>capers<br>prunes<br>banana leaves<br>salt and pepper",
    answers: [
      { text: 'Nicaragua', correct: true },
      { text: 'Kosovo', correct: false },
      { text: 'Mongolia', correct: false },
      { text: 'Portugal', correct: false },
      { text: 'Togo', correct: false },
      { text: 'USA', correct: false },
      { text: 'Qatar', correct: false },
      { text: 'Paraguay', correct: false }
    ]
  },  {
    question: "Where is this dish from?",
    imgSrc: "/img/129.jpeg",
    explanation: "This bread, called <i>taguella</i> (ⵜⴰⴳⵯⵍⵍ) in Tamasheq, is made in the sand of the Sahara Desert. It’s commonly eaten in Algeria, Tunisia, Libya, Chad, and <b>Niger</b>.",
    ingredList: "semolina flour<br>millet flour<br>salt<br>water<br>",
    answers: [
      { text: 'Mexico', correct: false },
      { text: 'Niger', correct: true },
      { text: 'Namibia', correct: false },
      { text: 'Saudi Arabia', correct: false },
      { text: 'Australia', correct: false },
      { text: 'China', correct: false },
      { text: 'Pakistan', correct: false },
      { text: 'Kyrgyzstan', correct: false }
    ]
  },  {
    question: "Where is this dish from?",
    imgSrc: "/img/186B.jpeg",
    explanation: "<i>Haggis</i> is a traditional dish from <b>Scotland</b>, made with parts of a sheep and boiled in sheep stomach. Yum!",
    ingredList: "sheep liver<br>sheep heart<br>sheep lungs<br>suet (fat)<br>oatmeal<br>onion<br>cayenne<br>sheep stomach",
    answers: [
      { text: 'USA', correct: false },
      { text: 'Ecuador', correct: false },
      { text: 'Scotland', correct: true },
      { text: 'Turkmenistan', correct: false },
      { text: 'Azerbaijan', correct: false },
      { text: 'Chad', correct: false },
      { text: 'Zambia', correct: false },
      { text: 'Indonesia', correct: false }
    ]
  },  {
    question: "Where is this dish from?",
    imgSrc: "/img/153-2.jpg",
    explanation: "<i>Lakh</i> is a dish made in <b>Senegal</b> for dessert and celebrations. It's made of millet, with a sweet soured milk sauce on top. It can be eaten hot or cold, but is often prepared with hot millet and cold milk. A variation of this dish, with peanuts and baobab (<i>ngalakh</i>), is prepared during Easter and sometimes given as an Easter gift from Muslims to their Christian neighbors and friends.",
    ingredList: "millet<br>curdled milk<br>sugar<br>concentrated milk<br>raisins",
    answers: [
      { text: 'Mexico', correct: false },
      { text: 'Croatia', correct: false },
      { text: 'Bolivia', correct: false },
      { text: 'Senegal', correct: true },
      { text: 'Brunei', correct: false },
      { text: 'India', correct: false },
      { text: 'Oman', correct: false },
      { text: 'Cameroon', correct: false }
    ]
  },  
  {
    question: "Where is this dish from?",
    imgSrc: "/img/169.jpeg",
    explanation: "<i>Surströmmingsklämma</i> is fermented herring (called <i>surströmming</i>) on hard or soft flatbread, eaten in <b>Switzerland</b> (outdoors, because it smells so bad).",
    ingredList: "fish<br>potatoes<br>tomatoes<br>red onion<br>sour cream<br>flatbread",
    answers: [
      { text: 'Netherlands', correct: false },
      { text: 'Fiji', correct: false },
      { text: 'Panama', correct: false },
      { text: 'St Kitts & Nevis', correct: false },
      { text: 'Sweden', correct: true },
      { text: 'Philippines', correct: false },
      { text: 'Ivory Coast', correct: false },
      { text: 'Singapore', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/180.jpeg",
    explanation: "This is <i>mantı</i> from <b>Turkey</b>!",
    ingredList: "flour<br>eggs<br>lamb or beef<br>onion<br>parsley<br>yogurt<br>garlic<br>tomatoes<br>butter<br>oil<br>water<br>salt<br>pepper",
    answers: [
      { text: 'Spain', correct: false },
      { text: 'Belgium', correct: false },
      { text: 'Tunisia', correct: false },
      { text: 'El Salvador', correct: false },
      { text: 'Cuba', correct: false },
      { text: 'Turkey', correct: true },
      { text: 'Slovakia', correct: false },
      { text: 'Uganda', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/193.jpeg",
    explanation: "<i>Phở </i> is a soup from <b>Vietnam</b> made with beef stock, charred spices, and meat, topped with some fresh herbs and sprouts. Here is a really lengthy treatise on its importance and preparation.",
    ingredList: "beef broth<br>brisket<br>sirloin<br>fish sauce<br>noodles<br>star anise<br>cinnamon<br>cardamom<br>cloves<br>fennel<br>lime<br>basil<br>bean sprouts",
    answers: [
      { text: 'Bulgaria', correct: false },
      { text: 'Egypt', correct: false },
      { text: 'Japan', correct: false },
      { text: 'Malaysia', correct: false },
      { text: 'Laos', correct: false },
      { text: 'Mauritania', correct: false },
      { text: 'Vietnam', correct: true },
      { text: 'Qatar', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/194.jpeg",
    explanation: "Saltah (سلتة) is stew from <b>Yemen</b>.",
    ingredList: "meat stew<br>fenugreek froth<br>chillies<br>tomatoes<br>garlic<br>herbs<br>rice<br>potatoes<br>scrambled eggs",
    answers: [
      { text: 'Bahrain', correct: false },
      { text: 'Romania', correct: false },
      { text: 'Zimbabwe', correct: false },
      { text: 'Latvia', correct: false },
      { text: 'Togo', correct: false },
      { text: 'Armenia', correct: false },
      { text: 'Albania', correct: false },
      { text: 'Yemen', correct: true }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/2.jpeg",
    explanation: "This lamb dish from <b>Albania</b> is called <i>tavë kosi</i>. It's a casserole made from lamb, rice, and a yogurt sauce.",
    ingredList: "lamb<br>rice<br>flour<br>yogurt<br>eggs<br>butter<br>nutmeg<br>garlic<br>oregano<br>",
    answers: [
      { text: 'Albania', correct: true },
      { text: 'Belize', correct: false },
      { text: 'Liberia', correct: false },
      { text: 'Macedonia', correct: false },
      { text: 'Libya', correct: false },
      { text: 'Taiwan', correct: false },
      { text: 'Djibouti', correct: false },
      { text: 'Latvia', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/3.jpeg",
    explanation: "<i>Zviti</i> is a tomato and pepper sauce from <b>Algeria</b>, which is grilled, mashed with a mortal and pestle, and served with a semolina flatbread.",
    ingredList: "semolina flour<br>salt<br>tomatoes<br>sweet pepper<br>hot peppers<br>tomato<br>garlic<br>coriander seeds<br>fennel seeds<br> butter",
    answers: [
      { text: 'Mexico', correct: false },
      { text: 'Algeria', correct: true },
      { text: 'Venezuela', correct: false },
      { text: 'Thailand', correct: false },
      { text: 'Serbia', correct: false },
      { text: 'Jordan', correct: false },
      { text: 'Guinea', correct: false },
      { text: 'Costa Rica', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/5.jpeg",
    explanation: "<i>Muamba de galinha</i> is a dish from <b>Angola</b> which is made with chicken, palm oil, and vegetables. It's often served with pirão (corn porridge) or funge (cassava porridge).",
    ingredList: "chicken<br>onion<br>okra<br>zucchini<br>palm oil<br>garlic<br>chili pepper",
    answers: [
      { text: 'Bangladesh', correct: false },
      { text: 'Honduras', correct: false },
      { text: 'Angola', correct: true },
      { text: 'Benin', correct: false },
      { text: 'Gabon', correct: false },
      { text: 'Montenegro', correct: false },
      { text: 'Haiti', correct: false },
      { text: 'Timor-Leste', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/6.jpeg",
    explanation: "<i>Duncana</i> is a sweet potato dumpling made in <b>Antigua and Barbuda</b>, often served with <i>saltfish</i>, which is salted and dried fish, soaked in water and cooked with vegetables.",
    ingredList: "coconut<br>sweet potatoes<br>sugar<br>cinnamon<br>nutmeg<br>flour<br>dried fish<br>salt<br>onions<br>garlic<br>peppers<br>tomato sauce<br>butter",
    answers: [
      { text: 'Georgia', correct: false },
      { text: 'UAE', correct: false },
      { text: 'Barbados', correct: false },
      { text: 'Antigua & Barbuda', correct: true },
      { text: 'Japan', correct: false },
      { text: 'Senegal', correct: false },
      { text: 'Belize', correct: false },
      { text: 'Sri Lanka', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/7.jpeg",
    explanation: "<i>Locro</i>, called <i>ruqru</i> in the Quechua langauge, is popular in <b>Argentina</b> and nearby countries of the Andes Mountains. It's a stew made of squash, corn, potatoes, and beans.",
    ingredList: "pork<br>beef<br>hominy<br>squash/pumpkin<br>potatoes<br>corn<br>beans<br> cumin<br>onions<br>paprika",
    answers: [
      { text: 'Congo', correct: false },
      { text: 'Eritrea', correct: false },
      { text: 'Ukraine', correct: false },
      { text: 'Canada', correct: false },
      { text: 'Argentina', correct: true },
      { text: 'Bangladesh', correct: false },
      { text: 'Belize', correct: false },
      { text: 'São Tomé & Príncipe', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/8.jpeg",
    explanation: "In <b>Armenia</b> and nearby countries, <i>harisa</i> (հարիսա) is often eaten on Easter or Eid, and it's traditionally given to poorer neighbors. It is made with wheat and chicken, and served with butter. ",
    ingredList: "chicken<br>peeled wheat<br>water<br>salt<br>butter",
    answers: [
      { text: 'Pakistan', correct: false },
      { text: 'Guatemala', correct: false },
      { text: 'Mali', correct: false },
      { text: 'Nepal', correct: false },
      { text: 'Papua New Guinea', correct: false },
      { text: 'Armenia', correct: true },
      { text: 'Australia', correct: false },
      { text: 'Uruguay', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/10.jpeg",
    explanation: "<b>Austria</b>'s <i>wiener schnitzel</i> is a battered and deep-fried veal cutlet. The name means <i>cutlet from Vienna</i> in German.",
    ingredList: "veal<br>flour<br>eggs<br>breadcrumbs<br>salt<br>oil<br>lemon",
    answers: [
      { text: 'Canada', correct: false },
      { text: 'Costa Rica', correct: false },
      { text: 'New Zealand', correct: false },
      { text: 'Nepal', correct: false },
      { text: 'Uganda', correct: false },
      { text: 'Argentina', correct: false },
      { text: 'Austria', correct: true },
      { text: 'Lebanon', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/.jpeg",
    explanation: "<i>Bughlama</i> is stew from <b>Azerbaijan</b>. It's made with lamb or other meat, with tomatoes and onions. Wouldn't Bill and Mary Sue be proud?",
    ingredList: "lamb<br>onions<br>tomatoes<br>peppers<br>tumeric<br>parsley<br>garlic<br>plums<br>lemon",
    answers: [
      { text: 'Rwanda', correct: false },
      { text: 'Tunisia', correct: false },
      { text: 'Great Britain', correct: false },
      { text: 'Finland', correct: false },
      { text: 'Vietnam', correct: false },
      { text: 'Greenland', correct: false },
      { text: 'Antigua & Barbuda', correct: false },
      { text: 'Azerbaijan', correct: true }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/13.jpeg",
    explanation: "<i>Muhammar</i> (محمر‎) is a sweet rice fish from <b>Bahrain</b> that is said to have been made by pearl divers, who used the dates as energy to better their dives.",
    ingredList: "basmati rice<br>dates<br>brown sugar<br>ghee<br>cardamom<br>saffron<br>rose water<br>salt",
    answers: [
      { text: 'Bahrain', correct: true },
      { text: 'Iceland', correct: false },
      { text: 'Sierra Leone', correct: false },
      { text: 'Malawi', correct: false },
      { text: 'Syria', correct: false },
      { text: 'Bhutan', correct: false },
      { text: 'Uzbekistan', correct: false },
      { text: 'Hong Kong', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/14.jpeg",
    explanation: "From <b>Bangladesh</b> and neighboring parts of India, <i>palta khirhuri</i> is a comfort food dish made of lentil stew served with rice. It's often eaten at Eid or during the monsoon season.",
    ingredList: "rice<br>lentils<br>split green peas<br>onions<br>turmeric<br>chilis<br>cumin<br>bay leaves<br>cardamom<br>cinnamon<br>coriander<br>",
    answers: [
      { text: 'France', correct: false },
      { text: 'Bangladesh', correct: true },
      { text: 'Kenya', correct: false },
      { text: 'Jordan', correct: false },
      { text: 'Hungary', correct: false },
      { text: 'Zambia', correct: false },
      { text: 'Thailand', correct: false },
      { text: 'Saudi Arabia', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/16.jpeg",
    explanation: "<i>Sashni</i> is just one of many potato dishes enjoyed in <b>Belarus</b>. This one is a simple recipe of mashes potatoes and eggs, with a cottage cheese and egg mixture inside.",
    ingredList: "potatoes<br>eggs<br>flour<br>cottage cheese",
    answers: [
      { text: 'Ireland', correct: false },
      { text: 'Poland', correct: false },
      { text: 'Belarus', correct: true },
      { text: 'Syria', correct: false },
      { text: 'Czech Republic', correct: false },
      { text: 'Greenland', correct: false },
      { text: 'USA', correct: false },
      { text: 'Bolivia', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/17.jpeg",
    explanation: "A cream sauce with poached chicken is popular in <b>Belgium</b>, and it's called <i>waterzooï de poulet</i>.",
    ingredList: "chicken<br>onion<br>carrot<br>celery<br>butter<br>bay leaf<br>fresh parsley<br> fresh thyme<br>chicken broth<br>potatoes>br>egg yolk<br>cream",
    answers: [
      { text: 'Serbia', correct: false },
      { text: 'Algeria', correct: false },
      { text: 'El Salvador', correct: false },
      { text: 'Belgium', correct: true },
      { text: 'Sweden', correct: false },
      { text: 'China', correct: false },
      { text: 'Malaysia', correct: false },
      { text: 'Pakistan', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/25.jpeg",
    explanation: "<i>Ambuyat</i> is a staple food and national dish of <b>Brunei</b>. It's a sticky mixture of sago palm and water, and it's often eaten with a spicy sauce called <i>cacah</i> and served with fish or other vegetables dishes. ambuyat is wrapped around chopsticks and swallowed whole!",
    ingredList: "sago palm<br>water<br>fish<br>chili<br>lime<br>shrimp paste<br>sugar<br>salt",
    answers: [
      { text: 'Trinidad & Tobago', correct: false },
      { text: 'Nigeria', correct: false },
      { text: 'Burundi', correct: false },
      { text: 'Sri Lanka', correct: false },
      { text: 'Brunei', correct: true },
      { text: 'Albania', correct: false },
      { text: 'Jamaica', correct: false },
      { text: 'Taiwan', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/27.jpeg",
    explanation: "<i>Babenda</i> is a rice and stewed leaf dish from <b>Burkina Faso</b>. It's made with sorrel, amaranth, and kinebdo leaves. Kinebdo is a local plant, unlikely to be found outside the region! It's sometimes served with tô, a thick ball or porridge made from millet or sorghum and water.",
    ingredList: "sorrel leaves<br>amaranth leaves<br>Kinebdo (cleone) leaves<br>peanuts<br>rice<br>millet or sorghum",
    answers: [
      { text: 'Rwanda', correct: false },
      { text: 'Jamaica', correct: false },
      { text: 'Philippines', correct: false },
      { text: 'Korea', correct: false },
      { text: 'Cambodia', correct: false },
      { text: 'Burkina Faso', correct: true },
      { text: 'Ethiopia', correct: false },
      { text: 'Jordan', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/30.jpeg",
    explanation: "A popular dish in <b>Cameroon</b> is <i>ndolé</i>, a stew made from bitterleaf and dried crustaceans, and often served with plantains.",
    ingredList: "shrimp<br>fish<br>beef<br>crayfish<br>bitterleaves<br>onion<br>peanuts<br>garlic<br>oil<br>plantains",
    answers: [
      { text: 'The Gambia', correct: false },
      { text: 'Israel', correct: false },
      { text: 'Bahrain', correct: false },
      { text: 'Nicaragua', correct: false },
      { text: 'Nepal', correct: false },
      { text: 'Ghana', correct: false },
      { text: 'Cameroon', correct: true },
      { text: 'Namibia', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/77.jpeg",
    explanation: "These are <i>bakso</i>, meatballs from <b>Indonesia</b>. They are often served with soup broth, noodles, and vegetables.",
    ingredList: "beef<br>cornflour<br>garlic<br>coriander<br>baking powder<br>soup broth<br>",
    answers: [
      { text: 'Russia', correct: false },
      { text: 'Chile', correct: false },
      { text: 'Togo', correct: false },
      { text: 'Greece', correct: false },
      { text: 'China', correct: false },
      { text: 'Afghanistan', correct: false },
      { text: 'USA', correct: false },
      { text: 'Indonesia', correct: true }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/140.jpeg",
    explanation: "This dish from <b>Poland</b> is called <i>kołduny</i>. It's the name of a dumpling made with ham and spices, and often served in a soup.",
    ingredList: "ham<br>flour<br>egg<br>salt<br>pepper<br>marjoram<br>garlic<br>paprika",
    answers: [
      { text: 'Poland', correct: true },
      { text: 'Great Britain', correct: false },
      { text: 'Serbia', correct: false },
      { text: 'Mexico', correct: false },
      { text: 'Thailand', correct: false },
      { text: 'Iraq', correct: false },
      { text: 'Qatar', correct: false },
      { text: 'Greece', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/151.jpeg",
    explanation: "A traditional dish of <b>São Tomé and Príncipe</b> and also enjoyed in Angola, <i>calulu de peixe</i> is a fish stew made with palm oil and okra, and often served with <i>funge</i> (cassava porridge).",
    ingredList: "fish<br>dried fish<br>onion<br>tomatoes<br>chili peppers<br>okra<br>sweet potato leaves<br>eggplant<br>okra<br>garlic<br>palm oil<br>lemon",
    answers: [
      { text: 'Spain', correct: false },
      { text: 'São Tomé & Príncipe', correct: true },
      { text: 'Cambodia', correct: false },
      { text: 'Cape Verde', correct: false },
      { text: 'Eritrea', correct: true },
      { text: 'Senegal', correct: false },
      { text: 'Malaysia', correct: false },
      { text: 'Guinea', correct: false } 
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/184.jpeg",
    explanation: "This bread loaf made in <b>Ukraine</b> and its neighboring countries, called <i>korovai</i> (коровай), is decorated with delicate flowers and leaves, and it's traditionally the first food eaten at a wedding, to bring the newlyweds good fortune.",
    ingredList: "yeast<br>milk<br>sugar<br>eggs<br>butter<br>wheat flour<br>salt",
    answers: [
      { text: 'New Zealand', correct: false },
      { text: 'Great Britain', correct: false },
      { text: 'Ukraine', correct: true },
      { text: 'Canada', correct: false },
      { text: 'Iceland', correct: true },
      { text: 'Mexico', correct: false },
      { text: 'Labanon', correct: false },
      { text: 'Brazil', correct: false }
    ]
  },
  {
    question: "Where is this dish from?",
    imgSrc: "/img/197.jpeg",
    explanation: "<i>Goat water</i> is a spicy stew from the island of <b>Montserrat</b> and also made on some nearby islands. It's thought to be a Carribbean take on an old Irish stew, and can be made with breadfruit as a substitute or addition to goat meat.",
    ingredList: "goat meat<br>molasses<br>cloves<br>pimento<br>garlic<br>onion<br>scotch bonnet chilis<br>pepper<br>flour<br>rum<br>",
    answers: [
      { text: 'New Zealand', correct: false },
      { text: 'Greece', correct: false },
      { text: 'Vatican', correct: false },
      { text: 'Montserrat', correct: true },
      { text: 'Peru', correct: true },
      { text: 'Panama', correct: false },
      { text: 'Azerbaijan', correct: false },
      { text: 'Japan', correct: false }
    ]
  }  
]