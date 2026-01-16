"use client";
import React from "react";
import { useMode } from "../context/theme";
import Image from "next/image";
import Logo from "../assets/Logo.svg";
import LogoDark from "../assets/LogoDark.svg";
import MoonLight from "../assets/moonLight.svg";
import MoonDark from "../assets/moon.svg";
import burger from "../assets/burgerMenu.svg";
import burgerWhite from "../assets/burgerMenuWhite.svg";

const Header = () => {
  const { isDark, setDark, setNavOpen } = useMode();
  return (
    <div
      className={`flex items-center z-10 p-5 pt-7 justify-between md:py-8 absolute w-full`}
    >
      <div className="logo flex items-center space-x-2">
        <Image src={!isDark ? Logo : LogoDark} width={25} />
        <p className="text-2xl font-bold">BRIX</p>
      </div>

      <ul className="flex-3 items-center flex space-x-5 hidden sm:flex">
        <li>
          <a href="#">HOME</a>
        </li>
        <li>
          <a href="#">ABOUT</a>
        </li>
        <li>
          <a href="#">EXPERIENCES</a>
        </li>
        <li>
          <a href="#">CONTACT</a>
        </li>
      </ul>

      <Image
        className="cursor-pointer hidden md:block"
        onClick={() => {
          setDark(!isDark);
        }}
        src={isDark ? MoonLight : MoonDark}
        width={25}
      />
      <Image
        className="cursor-pointer md:hidden"
        src={isDark ? burgerWhite : burger}
        onClick={() => {
          setNavOpen(true);
        }}
        width={25}
      />
    </div>
  );
};

export default Header;