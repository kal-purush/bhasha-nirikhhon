var express = require('express');
var router = express.Router();
var path = require('path');
var admin = require(path.join(__dirname, '../modules/admin'));

router.get('/', function (req, res) {
  var sess = req.session;
  if (!sess.login_flg) {
    res.redirect('/admin/login');
    return;
  }
  res.redirect('/admin/info');
});

router.get('/login', function (req, res) {
  var sess = req.session;
  if (sess.login_flg) {
    res.redirect('/admin/');
    return;
  }
  res.render('admin/login', { title: 'Wan Chaofan - admin login' });
});

router.post('/login', function (req, res) {
  var sess = req.session;
  if (req.body.login) {
    admin.login(req.body.user, req.body.pass, function (result) {
      if (result === "error") {
        res.render('admin/login', { title: 'Wan Chaofan - admin login', message: 'login failed unexpectedly' });
        return;
      }
      if (result === "failed") {
        res.render('admin/login', { title: 'Wan Chaofan - admin login', message: 'username/password wrong' });
        return;
      }
      if (result === "success") {
        sess.login_flg = 1;
        res.redirect('/admin/info');
        return;
      }
    });
  } else {
    res.render('admin/login', { title: 'Wan Chaofan - admin login', message: 'login failed without push login button' });
  }
});

router.get('/info', function (req, res) {
  var sess = req.session;
  if (!sess.login_flg) {
    res.redirect('/admin/login');
    return;
  }

  admin.getInfo(function (user_info) {
    res.render('admin/info', { section: "info", title: 'Wan Chaofan - admin - info', user_info: user_info });
  });
});

router.post('/info', function (req, res) {
  var sess = req.session;
  if (!sess.login_flg) {
    res.redirect('/admin/login');
    return;
  }
  if (req.body.save) {
    admin.getInfo(function (update_data) {
      update_data.contact.phone = req.body.phone;
      update_data.contact.email = req.body.email;
      update_data.bio = req.body.bio;
      admin.changeInfo(update_data, function (user_info) {
        var error_message = "";
        if (user_info === "error") {
          error_message = "change failed";
        }
        res.render('admin/info', { section: "info", title: 'Wan Chaofan - admin - info', user_info: user_info, error_message: error_message });
      });
    });
  } else {
    admin.getInfo(function (user_info) {
      var error_message = "post save info in expected way";
      res.render('admin/info', { section: "info", title: 'Wan Chaofan - admin - info', user_info: user_info, error_message: error_message });
    });
  }
});

router.get('/logout', function (req, res) {
  req.session.destroy();
  res.redirect('/admin/login');
});

module.exports = router;