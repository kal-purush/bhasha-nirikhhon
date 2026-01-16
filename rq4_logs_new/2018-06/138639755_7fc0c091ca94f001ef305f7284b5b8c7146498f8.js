exports.seed = function(knex, Promise) {
  return knex('palettes').del()
    .then(() => knex('projects').del())
    .then(() => {
      return Promise.all([
        knex('projects').insert({ name: 'My Fake Project' }, 'id')
        .then(project => {
          return knex('palettes').insert([
            { name: 'greens', 
              color1: '#111111', 
              color2: '#222222', 
              color3: '#333333', 
              color4: '#444444', 
              color5: '#555555', 
              project_id: project[0] 
            },
            { name: 'pride', 
              color1: '#FFFFFF', 
              color2: '#212121', 
              color3: '#666666', 
              color4: '#777777', 
              color5: '#888888', 
              project_id: project[0] 
            }
          ])
        })
        .then(() => console.log('Seeding complete!'))
        .catch(error => console.log(`Error seeding data: ${ error }`))
      ])
    })
    .catch(error => console.log(`Error seeding data: ${ error }`))
};