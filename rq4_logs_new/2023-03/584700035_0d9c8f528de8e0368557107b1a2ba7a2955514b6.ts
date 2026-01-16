export function firstVisitCheck(){
  const isVisit = window.localStorage.getItem('visit');
  if(!isVisit) return true;
  else return false;
};