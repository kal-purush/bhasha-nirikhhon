const express = require('express')
const path = require('path')
const bodyParse = require('body-parser')
const moment = require('moment')

// mysql
const mysql = require('mysql')
const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: '',
  port: 3306,
  database: 'movies',
  connectionLimit: 10
})

const port = process.env.PORT || 8000
const app = express()

app.set('views', './views/pages')
app.set('view engine', 'jade')
app.locals.moment = moment
// 因后台录入页有提交表单的步骤，故加载此模块方法（bodyParser模块来做文件解析），将表单里的数据进行格式化
app.use(bodyParse.urlencoded({extended: true}))
app.use(express.static(path.join(__dirname, 'public')))

app.listen(port)
console.log('your url are http://127.0.0.1:' + port);


// 编写页面路由
// index
app.get('/', (req, res) => {
  let start = req.query.page || 0
  pool.getConnection((err, conn) => {
    if (err) {
      console.log(err)
    } else {
      conn.query(`SELECT * FROM movie LIMIT ${start}, 10`, (err, movies) => {
        if (err) {
          console.log(err)
        } else {
          res.render('index', {
            title: '首页',
            movies: movies
          });
        }
        conn.release()
      })
    }
  })
})
// detail
app.get('/movie/:id', (req, res) => {
  let mid = req.params.id
  console.log('mid', req.url)
  pool.getConnection((err, conn) => {
    if (err) {
      console.log(err)
    } else {
      conn.query(`SELECT * FROM movie WHERE mid = ?`, [mid], (err, movie) => {
        if (err) {
          console.log( err)
        } else {
          console.log(movie)
          res.render('detail', {
            title: '首页',
            movie: movie[0]
          })
        }
        conn.release()
      })
    }
  })
})
// admin
app.get('/admin/movie', (req, res) => {
  let mid = req.query.id
  if (mid) {
    pool.getConnection((err, conn) => {
      if (err) {
        console.log(err)
      } else {
        conn.query(`SELECT * FROM movie WHERE mid = ?`, [mid], (err, movies) => {
          if (err) {
            console.log(err)
          } else {
            res.render('admin', {
              title: 'list',
              movie: movies[0]
            });
          }
          conn.release()
        })
      }
    })
  } else {
    res.render('admin', {
      title: 'admin',
      movie: {
        title: '',
        doctor: '',
        country: '',
        years: '',
        poster: '',
        flash: '',
        summary: '',
        language: ''
      }
    })
  }
})
// app.post('/admin/update', (req, res) => {
//   let movie = req.body.movie
//   pool.getConnection((err, conn) => {
//     if (err) {
//       console.log(err)
//     } else {
      
//     }
//   })
// })
app.post('/admin/movie/new', (req, res) => {
  let movie = req.body.movie
  let mid = movie.mid
  pool.getConnection((err, conn) => {
    if (err) {
      console.log(err)
    } else {
      console.log('mid', mid)
      if (mid !== 'undefined') {
        conn.query(`UPDATE movie SET title=?, doctor=?, language=?, country=?, summary=?, flash=?, poster=?, updateAt=? WHERE mid = ?`, [movie.title, movie.doctor, movie.language, movie.country, movie.summary, movie.flash, movie.poster, new Date(), mid], (err, result) => {
          if (err) {
            console.log( err)
          } else {
            res.redirect('/movie/' + movie.mid)
          }
          conn.release()
        })
      } else {
        conn.query(`INSERT INTO movie VALUES(NULL, '${movie.title}', '${movie.doctor}', '${movie.language}', '${movie.country}', '${movie.summary}', '${movie.flash}', '${movie.poster}', '2018', now(), now());`, (err, movie) => {
          if (err) {
            console.log( err)
          } else {
            console.log('new movie', movie)
            res.redirect('/movie/' + movie.insertId)
          }
          conn.release()
        })
      }
      
    }
  })
})
// list
app.get('/admin/list', (req, res) => {
  let start = req.query.page || 0
  pool.getConnection((err, conn) => {
    if (err) {
      console.log(err)
    } else {
      conn.query(`SELECT * FROM movie LIMIT ${start}, 10`, (err, movies) => {
        if (err) {
          console.log(err)
        } else {
          res.render('list', {
            title: 'list',
            movies: movies
          });
        }
        conn.release()
      })
    }
  })
})
app.delete('/admin/delete', (req, res) => {
  let mid = req.query.id || 0
  pool.getConnection((err, conn) => {
    if (err) {
      console.log(err)
    } else {
      conn.query(`DELETE FROM movie WHERE mid = ?`, [mid], (err, movies) => {
        if (err) {
          console.log(err)
        } else {
          res.json({
            success: 1
          })
        }
        conn.release()
      })
    }
  })
})