const brainEvenTask = () => {
  const q = Math.round(Math.random() * 100);
  // const a = q % 2; // Вернет 1 если нечет и вернет 0 если чет
  let a = '';
  if (q % 2 === 0) {
    a = 'yes';
  } else {
    a = 'no';
  }
  return [q, a];
};

export default brainEvenTask;