import React from "react";
import Image from "next/image";
import Link from "next/link";

const Members = () => {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 p-2 gap-4">
      <div className="w-[12rem] h-[8rem]  flex flex-col justify-center items-center ">
        <Link href="https://lbeyewear.co.za/">
          <Image
            src="/images/lb-eyewear.png"
            alt="Logo"
            width={200}
            height={10}
            className="inline-block cursor-pointer"
          />
        </Link>
      </div>
      <div className="w-[12rem] h-[8rem]  flex flex-col justify-center items-center">
        <Link href="https://waterfalleyecare.co.za/">
          <Image
            src="/images/waterfall-eyecare.png"
            alt="Logo"
            width={200}
            height={10}
            className="inline-block cursor-pointer"
          />
        </Link>
      </div>
      <div className="w-[12rem] h-[8rem]   flex flex-col justify-center items-center">
        <Link href="https://winkoptom.co.za/">
          <Image
            src="/images/wink-optom.png"
            alt="Logo"
            width={200}
            height={10}
            className="inline-block cursor-pointer"
          />
        </Link>
      </div>
      <div className="w-[12rem] h-[8rem]   flex flex-col justify-center items-center">
        <Link href="https://wedgeoptom.com/">
          <Image
            src="/images/bev-optom.png"
            alt="Logo"
            width={200}
            height={10}
            className="inline-block cursor-pointer"
          />
        </Link>
      </div>
    </div>
  );
};

export default Members;