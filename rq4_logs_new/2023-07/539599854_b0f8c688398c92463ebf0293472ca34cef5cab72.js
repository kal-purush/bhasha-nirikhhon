// json data -- how can I import this???
// the instructions in the README and online seem to conflict :(
const cardsJSON = '[ { "name": "Paolo Maldini", "jobTitle": "Front End Developer", "company": "Web Weavers", "experience": "3 years", "school": "University of Washington", "major": "Marketing", "email": "paolo@example.com", "linkedInUrl": "paolo.linkedinprofile.com", "codeLanguages": [ "HTML", "CSS", "JavaScript", "Node", "Express" ] }, { "name": "Barbara Bonansea", "jobTitle": "Software Engineer", "company": "Excellence in the Cloud", "experience": "12 years", "school": "University of Southern California", "major": "Computer Science", "email": "barbara@example.com", "linkedInUrl": "barbara.linkedinprofile.com", "codeLanguages": [ "HTML", "JavaScript", "C", "Go" ] }, { "name": "Javier Hernandez", "jobTitle": "User Experience Engineer", "company": "Web Sites and More", "experience": "5 years", "school": "Seattle University", "major": "Performing Arts", "email": "javier@example.com", "linkedInUrl": "javier.linkedinprofile.com", "codeLanguages": [ "HTML", "CSS" ] }, { "name": "Maribel Dominguez", "jobTitle": "Front End Engineer", "company": "Bits and Bytes", "experience": "6 years", "school": "University of Washington", "major": "Mechanical Engineering", "email": "maribel@example.com", "linkedInUrl": "maribel.linkedinprofile.com", "codeLanguages": [ "HTML", "CSS", "JavaScript", "React", "Vue", "Redux" ] } ]'

// parse into js object
const cards = JSON.parse(cardsJSON)

// get html container for population
const container = document.querySelector('.template-hook')

// create card class constructor
class Card {
  // expected parameters
  constructor(name, jobTitle, company, experience, school, major, email, linkedInUrl, codeLanguages) {
    // set equal to instance -- there must be a better way to do this?
    this.name = name
    this.jobTitle = jobTitle
    this.company = company
    this.experience = experience
    this.school = school
    this.major = major
    this.email = email
    this.linkedInUrl = linkedInUrl
    this.codeLanguages = codeLanguages // for bonus if time allows
  }
  // card creation method
  createCard() {
    let cardHTML = document.createElement('div')
    cardHTML.classname = 'card'
    cardHTML.innerHTML = `
      <div class="photo">
        <img src="img/headshot.jpg" alt="Placeholder Headshot">
        <h2 class="name">${this.name}</h2>
        <h3 class="title">${this.jobTitle}</h3>
      </div>
      <div class="info">
        <ul>
          <li><b>Company:</b> ${this.company}</li>
          <li><b>Experience:</b> ${this.experience}</li>
          <li><b>School:</b> ${this.school}</li>
          <li><b>Major:</b> ${this.major}</li>
          <li><b>Email:</b> <a href="mailto:${this.email}</a></li>
          <li><img src="img/linkedin.svg" alt="LinkedIn Icon"> <a href="${this.linkedInUrl}">${this.linkedInUrl}</a></li>
        </ul>
      </div>
    `
    // append created html block to container
    container.append(cardHTML)
  }
}

for (card in cards) {

}