const login = (req, res) => {
  const { username, password } = req.body

  if (username === 'admin' && password === 'admin') {
    req.session.user = username

    res.cookie('sessionId', req.sessionID)
    return res.status(200).json({ msg: 'logged in' })
  }

  else
    return res.status(400).json({ errors: ['username or password is invalid'] })
}

const logout = (req, res) => {
  req.session.destroy(() => {
    res.clearCookie('sessionId')
    return res.status(200).json({ msg: 'logged out' })
  })
}

module.exports = { login, logout }