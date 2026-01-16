import ComicBookThumbnail from '../assets/images/comicbook.gif';
import ComicBookImage from '../assets/images/comic-book-finder.png';
import BarkMatchThumbnail from '../assets/images/barkmatch.gif';
import BarkMatchImage from '../assets/images/barkmatch.png';
import EatDaBurgerThumbnail from '../assets/images/eat-da-burger.gif';
import EatDaBurgerImage from '../assets/images/eat-da-burger.PNG';
import TimedQuizThumbnail from '../assets/images/timed-quiz.gif';
import TimedQuizImage from '../assets/images/timed-quiz.png';
import WeatherThumbnail from '../assets/images/weather-dashboard.gif';
import WeatherImage from '../assets/images/weather-dashboard.png';

const projects = [
    {
        title: "Comicbook Finder",
        thumbnail: ComicBookThumbnail,
        image: ComicBookImage,
        details: "This project was created in a collaboration with me and three other developers with our code hosted in GitHub. The frontend was created using HTML 5 and styled mainly using the Bootstrap framework along with some custom CSS. The backend uses jQuery to call two API's, Comicvine and SuperheroAPI, to retrieve data about the characters and comic books. The page is then populated with elements dynamically created from the clientside Javascript.",
        description: "A single page website created to find random characters and comic books that the characters made appearances in.",
        url: 'https://raiford2530.github.io/ComicBook-Finder/',
        repo: 'https://github.com/raiford2530/ComicBook-Finder',
        technologies: ['Git', 'HTML 5', 'jQuery', 'CSS', 'Bootstrap', 'Javascript']
      },
      {
          title: "Bark Match",
          thumbnail: BarkMatchThumbnail,
          image: BarkMatchImage,
          details: 'Barkmatch was created using Node.js and several node packages. The project used the express node package to create the server, view routes, and api routes. The express handlebars node package was used to render templates created on the server side with data pulled in from the database. The Sequelize node package was used for all of the queries to the MySql database. We used node NodeMailer to create and send a confirmation email to a user once they sign up to walk a dog. Authentication was implemented using the Passport node package. All of the styling was done using the Bulma css framework along with custom css.',
          description: "BarkMatch is a website that allows users to browse dogs and sign up to walk a dog from a list of dogs uploaded by a dog shelter",
          url: 'https://vast-river-69240.herokuapp.com/',
          repo: 'https://github.com/raiford2530/BarkMatch-project2',
          technologies: ['Git', 'Node.js', 'jQuery', 'CSS', 'Bulma', 'Express', 'Express Handlebars', 'MySQL', 'Sequelize', 'NodeMailer', 'Passport']
        },
        {
          title: "Eat-Da-Burger",
          thumbnail: EatDaBurgerThumbnail,
          image: EatDaBurgerImage,
          details: 'Eat-Da-Burger was created using Node.js. I used the express node package for to create view routes and api routes. For the view routes, I used the express handlebars node package to render templates created on the server side. I created an ORM to mediate calls to the MySQL database to create and modify data about the burgers input by the user.',
          description: "Eat-Da-Burger is a simple web app for users to enter burgers that they want to eat and then indicate when they have eaten the burgers",
          url: 'https://rraiford-eat-a-burger.herokuapp.com',
          repo: 'https://github.com/raiford2530/burger',
          technologies: ['Git', 'Node.js', 'jQuery', 'CSS', 'Bootstrap', 'Express', 'Express Handlebars' ,'MySQL']
        },
        {
          title: "Weather Dashboard",
          thumbnail: WeatherThumbnail,
          image: WeatherImage,
          details: "The frontend was created using HTML 5 and styled mainly using the Bootstrap framework along with some custom CSS. The backend uses jQuery to call the OpenWeather API to retrieve the weather data for the searched city. The page is then populated with elements dynamically created from the clientside Javascript. The page uses local storage to store the user's last search so that it persists when the user leaves or refreshes the page.",
          description: "A website created to find current weather conditions and the five day weather forecast for a given city.",
          url: 'https://raiford2530.github.io/weather-dashboard/',
          repo: 'https://github.com/raiford2530/weather-dashboard',
          technologies: ['Git', 'HTML 5', 'jQuery', 'CSS', 'Bootstrap', 'Javascript']
        },
        {
          title: "Timed Quiz",
          thumbnail: TimedQuizThumbnail,
          image: TimedQuizImage,
          details: "The frontend was created using HTML 5 and styled mainly using the Bootstrap framework along with some custom CSS. I used clientside Javascript to handle the timer and to populate the questions with questions and answers. The webpage uses local storage to store the user's score and display it in the list of highscores on the highscore page.",
          description: "A website created for a user to take a timed quiz over javascript terms.",
          url:'https://raiford2530.github.io/timed-quiz/',
          repo:'https://github.com/raiford2530/timed-quiz',
          technologies: ['Git', 'HTML 5', 'CSS', 'Bootstrap', 'Javascript']
        }
    ]

    export default projects;