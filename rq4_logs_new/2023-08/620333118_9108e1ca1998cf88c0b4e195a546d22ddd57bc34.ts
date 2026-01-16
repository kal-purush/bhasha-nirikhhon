type DateAndTime = {
  date: string;
  time: string;
};

const getDateAndTime = (): DateAndTime => {
  const now = new Date();

  const year = now.getFullYear();
  const month = (now.getMonth() + 1).toString().padStart(2, '0'); // 0 ~ 11
  const day = now.getDate().toString().padStart(2, '0');
  const hour = now.getHours().toString().padStart(2, '0');
  const minute = now.getMinutes().toString().padStart(2, '0');

  const date = `${year}-${month}-${day}`;
  const time = `${hour}:${minute}`;

  return {
    date,
    time,
  };
};

export default getDateAndTime;