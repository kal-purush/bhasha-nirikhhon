export default function defaults(obj, ...args) {
  return Object.assign({}, ...args.reverse(), obj);
}