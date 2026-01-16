import Link from "next/Link";
import Image from "next/image";

export default function Header() {
  return (
    <header>
      <div className='container mx-auto flex flex-wrap p-5 flex-col md:flex-row items-center'>
        <Link href='/'>
          <a className='flex md:w-1/5  title-font font-medium items-center md:justify-start mb-4 md:mb-0'>
            {" "}
            <Image
              src='/favicon-32x32.png'
              alt='Logo'
              width={40}
              height={40}
            />{" "}
          </a>
        </Link>
        <nav className='flex flex-wrap md:w-4/5 items-center justify-end text-base md:ml-auto'>
          <Link href='/notes'>
            <a className='mx-5 cursor-pointer uppercase hover:text-indigo-300 tracking-wider'>
              {" "}
              Notes{" "}
            </a>
          </Link>
          <Link href='/about'>
            <a className='mx-5 cursor-pointer uppercase hover:text-indigo-300 tracking-wider'>
              {" "}
              About{" "}
            </a>
          </Link>
        </nav>
      </div>
    </header>
  );
}