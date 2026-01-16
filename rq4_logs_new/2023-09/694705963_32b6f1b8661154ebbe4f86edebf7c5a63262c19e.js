/* 
1. creare l'array di oggetti con le informazioni fornite.
2. Stampare su console, per ogni membro del team, le informazioni di nome, ruolo e la stringa della foto
3. Stampare le stesse informazioni su DOM sottoforma di stringhe */

const teams = 
              [{
                //1.
                nome:'Wayne Barnett',
                ruolo:'Founder & CEO',
                foto:'wayne-barnett-founder-ceo.jpg'
                },

                {//2.
                  nome:'Angela Caroll',
                  ruolo:'Chief Editor',
                  foto:'angela-caroll-chief-editor.jpg'
                  },

                {//3.
                  nome:'Walter Gordon',
                  ruolo:'Office Manager',
                  foto:'walter-gordon-office-manager'
                },

                {//4.
                  nome:'Angela Lopez',
                  ruolo:'Social Media Manager',
                  foto:'angela-lopez-social-media-manager.jpg'
                },

                {//5.
                  nome:'Scott Estrada',
                  ruolo:'Developer',
                  foto:'scott-estrada-developer.jpg'
                },

                {//6.
                  nome:'Barbara Ramos',
                  ruolo:'Graphic Designer',
                  foto:'barbara-ramos-graphic-designer.jpg'
                }
              ]

for(let member of teams){
  console(member);
  console(member.nome)
  console(member.ruolo)
  console(member.foto)
}