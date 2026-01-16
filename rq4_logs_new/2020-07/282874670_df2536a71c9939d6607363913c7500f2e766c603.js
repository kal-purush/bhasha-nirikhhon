export default function convertToAngka(rupiah) {
  return parseInt(rupiah.replace(/,.*|[^0-9]/g, ""), 10);
}