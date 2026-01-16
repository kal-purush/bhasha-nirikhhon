import Image from "next/image";
export default function Logo() {
  return (
    <div>
      <Image src="/Film_Finder.svg" width={100} height={100} alt="Brand Logo" />
    </div>
  );
}