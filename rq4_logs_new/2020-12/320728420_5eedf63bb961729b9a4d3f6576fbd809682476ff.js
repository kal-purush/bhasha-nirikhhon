const indexCtrl = {}

indexCtrl.renderHome = (req, res) => {
  res.render('home', { home: 'home' })
}

module.exports = indexCtrl