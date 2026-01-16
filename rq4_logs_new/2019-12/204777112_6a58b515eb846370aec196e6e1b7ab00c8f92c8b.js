import React from 'react';

import { connect } from 'react-redux';

import { Redirect } from 'react-router-dom';

import { createStructuredSelector } from 'reselect';

import { selectCurrentUserType } from 'redux/user/user.selectors';

import StudentLanding from 'pages/after-login-pages/student/student-landing/student-landing.page';
import TeacherLanding from 'pages/after-login-pages/teacher-admin/teacher-landing/teacher-landing-page';
import AdminLanding from 'pages/after-login-pages/teacher-admin/admin-landing/admin-landing.page';

const AuthenticatedApp = ({ userType }) => (
  <div>
    {
      {
        alumno: <StudentLanding />,
        profesor: <TeacherLanding />,
        admin: <AdminLanding />
      }[userType]
    }
  </div>
);

const mapStateToProps = createStructuredSelector({
  userType: selectCurrentUserType
});

export default connect(mapStateToProps)(AuthenticatedApp);