const sequelize = require('../config/connection');
const { Profile, Posts} = require('../models');



const seed = async () => {
  await sequelize.sync({ force: true })

  await Profile.bulkCreate([
    {
      id: 1,
      username: "KatyVincent",
      password: "password"
    },
    {
      id: 2,
      username: "DizzyVincent",
      password: "password1"
    }
  ])

  await Posts.bulkCreate([
    {
      id: 1,
      user_id: 1,
      blog_title: "How to Code Websites",
      blog_content: "Have Katy do it.",
    },
    {
      id: 2,
      user_id: 2,
      blog_title: "How to Be A Dog",
      blog_content: "Be Dizzy",
    }
  ])
  process.exit()
}

seed()