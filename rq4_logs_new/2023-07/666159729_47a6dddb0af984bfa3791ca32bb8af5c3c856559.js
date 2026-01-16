const BASE_URL = 'https://api.adviceslip.com/advice';

const fetchAdvice = async () => {
  try {
    const response = await fetch(BASE_URL);
    const data = await response.json();
    return data;
  } catch (error) {
    return new Error('Something was wrong. Try later please.');
  }
};

export default fetchAdvice;