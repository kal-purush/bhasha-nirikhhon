export function ImageDetail({ image }) {
  return (
    <img
      src={`/${image}.jpg`}
      className="object-cover rounded shadow-lg h-[66vh] w-full"
    />
  );
}