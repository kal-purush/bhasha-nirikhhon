import Qualification from 'types/qualification';

export const alevels: Qualification = {
  date: '1992',
  description: 'Maths (A), Further Maths (C), Physics (C), Business Studies (C)',
  location: 'Taunton',
  name: '4 A-Levels',
  school: 'Taunton School'
};
export const degree: Qualification = {
  date: '1996',
  location: 'Bath',
  name: 'Mathematics & Computing BSc (Hons)',
  school: 'University of Bath'
};
export const gcses: Qualification = {
  date: '1990',
  description:
    'Maths (A), Physics (A), Chemistry (A), ' +
    'Additional Maths (B), French (B), Biology (B), ' +
    'English Language (C), English Literature (C), Business Studies (C)',
  location: 'Taunton',
  name: '9 GCSEs',
  school: 'Taunton School'
};
export const scrumMaster: Qualification = {
  date: '2016',
  description: 'Attended a 2-day course before taking the exam at home',
  location: 'London',
  name: 'Professional Scrum Master I (PSM I)',
  school: 'Scrum.org',
  url: 'https://www.credly.com/badges/999fd331-1153-45df-a9a7-27a9145d0488/public_url'
};

// The order determines the order they are displayed
const myEducation: Qualification[] = [scrumMaster, degree, alevels, gcses];

export default myEducation;