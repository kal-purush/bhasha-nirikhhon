import myCv from 'content/cv/myCv';
import myEducation from 'content/cv/myEducation';
import myEmployment from 'content/cv/myEmployment';
import myPortfolio from 'content/cv/myPortfolio';
import myProfile from 'content/cv/myProfile';
import mySkills from 'content/cv/mySkills';

describe('myCv', () => {
  test('contains my profile', () => {
    expect(myCv.profile).toBe(myProfile);
  });

  test('contains my skills', () => {
    expect(myCv.skills).toBe(mySkills);
  });

  test('contains my portfolio', () => {
    expect(myCv.portfolio).toBe(myPortfolio);
  });

  test('contains my employment', () => {
    expect(myCv.employment).toBe(myEmployment);
  });

  test('contains my education', () => {
    expect(myCv.education).toBe(myEducation);
  });
});