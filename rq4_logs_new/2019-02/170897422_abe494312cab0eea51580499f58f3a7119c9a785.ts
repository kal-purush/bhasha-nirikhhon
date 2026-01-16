interface Collection {
  name?: string;
  state?: string;
}

export default function cascadeAnimation(
  collection: Collection[],
  index: number = 0,
  initialDelay: number = 0,
  delay: number = 150) {

  if (index >= collection.length) return false;

  setTimeout(() => {
    collection[index].state = 'final';
    console.log(collection[index].name);
    cascadeAnimation(collection, ++index, 0, delay);
  }, initialDelay + delay);
}