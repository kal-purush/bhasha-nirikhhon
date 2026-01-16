const getEntrySetter = (entries, stateSetter, index) => (entry) => {
  if (index === undefined) {
    return;
  }
  const entriesCopy = [...entries];
  const newEntries = entriesCopy.splice(index, 1, entry);
  stateSetter(newEntries);
};

export default getEntrySetter;