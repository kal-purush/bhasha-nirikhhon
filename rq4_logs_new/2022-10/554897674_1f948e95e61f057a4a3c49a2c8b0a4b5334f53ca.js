// Utilizzando i dati forniti, creare un array di oggetti per rappresentare i membri del team.
// Ogni membro è caratterizzato dalle seguenti informazioni: nome, ruolo e foto.

// Wayne Barnett	Founder & CEO	        wayne-barnett-founder-ceo.jpg
// Angela Caroll	Chief Editor	        angela-caroll-chief-editor.jpg
// Walter Gordon	Office Manager	        walter-gordon-office-manager.jpg
// Angela Lopez	Social Media Manager	angela-lopez-social-media-manager.jpg
// Scott Estrada	Developer	            scott-estrada-developer.jpg
// Barbara Ramos	Graphic Designer	    barbara-ramos-graphic-designer.jpg
// MILESTONE 0:
// Creare l’array di oggetti con le informazioni fornite.
// MILESTONE 1:
// Stampare su console le informazioni di nome, ruolo e la stringa della foto
// MILESTONE 2:
// Stampare le stesse informazioni su DOM sottoforma di stringhe
// BONUS 1:
// Trasformare la stringa foto in una immagine effettiva
// BONUS 2:
// Organizzare i singoli membri in card/schede. Se non vi sentite particolarmente creativi, potete prendere uno spunto dallo screenshot allegato.
// Consigli del giorno:
// Ragioniamo come sempre a step.
// Prima la logica in italiano e poi traduciamo in codice.
// E ricordiamoci che console.log() è nostro amico!
//////////////////////////////////
/////////////////////////////////
// milestone 0
const membersTeamName = [
  "Wayne",
  "Angela",
  "Walter",
  "Angela",
  "Scott",
  "Barbara",
];
const membersTeamSurname = [
  "Barnett",
  "Caroll",
  "Gordon",
  "Lopez",
  "Estrada",
  "Ramos",
];
const membersTeamRole = [
  "Founder & Ceo",
  "Chief Editor",
  "Office Manager",
  "Social Media Manager ",
  "Developer",
  "Graphic Designer",
];
const membersTeamFoto = [
  "wayne-barnett-founder-ceo.jpg",
  "angela-caroll-chief-editor.jpg",
  "walter-gordon-office-manager.jpg",
  "angela-lopez-social-media-manager.jpg",
  "scott-estrada-developer.jpg",
  "barbara-ramos-graphic-designer.jpg",
];
const firstMembersTeam = {
  name: "Wayne",
  lastname: "Barnett",
  role: "Founder & Ceo",
  foto: "wayne-barnett-founder-ceo.jpg",
};
const secondMembersTeam = {
  name: "Angela",
  lastname: "Caroll",
  role: "Chief Editor",
  foto: "angela-caroll-chief-editor.jpg",
};
const thirdMembersTeam = {
  name: "Walter",
  lastname: "Gordon",
  role: "Office Manager",
  foto: "walter-gordon-office-manager.jpg",
};
const fourthMembersTeam = {
  name: "Angela",
  lastname: "Lopez",
  role: "Social Media Manafer",
  foto: "angela-lopez-social-media-manager.jpg",
};
const fifthMembersTeam = {
  name: "Scott",
  lastname: "Estrada",
  role: "Developer",
  foto: "scott-estrada-developer.jpg",
};
const sixthMembersTeam = {
  name: "Barbara",
  lastname: "Ramos",
  role: "Graphic Desiner",
  foto: "barbara-ramos-graphic-designer.jpg",
};
//////////////////////////////
/////////////////////////////
// milestone 1
const membersArray = [
  "firstMembersTeam",
  "secondMembersTeam",
  "thirdMembersTeam",
  "fourthMembersTeam",
  "fifthMembersTeam",
  "sixthMembersTeam",
];
console.log(membersArray.name.lastname.role.foto);

// membersArray.firstMembersTeam.secondMembersTeam.thirdMembersTeam
//   .fourthMembersTeam.fifthMembersTeam.sixthMembersTeam