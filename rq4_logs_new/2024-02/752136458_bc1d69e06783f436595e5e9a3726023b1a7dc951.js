const fetchData = async () => {
  const response = await fetch("https://randomuser.me/api").then((res) =>
    res.json()
  );
  const {
    email,
    name: { last, first, title },
  } = response.results[0];
};
fetchData();