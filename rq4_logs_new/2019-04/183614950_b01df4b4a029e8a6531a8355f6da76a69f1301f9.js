module.exports = (knex, query, body) => {
  return () => {
    return Promise.resolve(
      knex("users")
        .where(query)
        .update(body)
        .then((users) => {
          return users;
        })
    );
  };
};