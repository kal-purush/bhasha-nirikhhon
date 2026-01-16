const Footer = () => {
  return (
    <>
      <div className="w-[80%] mx-auto flex justify-between">
        <div className="w-[25%]">
          <ul className="">
            <li className="text-[15px] font-[400] text-[#94A3B8] cursor-pointer">
              COMPANY
            </li>
            <li className="text-[13px] font-[400] pt-4 cursor-pointer">
              Features
            </li>
            <li className="text-[13px] font-[400] pt-4 cursor-pointer">
              Works
            </li>
            <li className="text-[13px] font-[400] pt-4 cursor-pointer">
              Career
            </li>
          </ul>
        </div>
        <div className="w-[25%]">
          <ul className="">
            <li className="text-[15px] font-[400] text-[#94A3B8] cursor-pointer">
              HELP
            </li>
            <li className="text-[13px] font-[400] pt-4 cursor-pointer">
              Customer Support
            </li>
            <li className="text-[13px] font-[400] pt-4 cursor-pointer">
              Delivery Details
            </li>
            <li className="text-[13px] font-[400] pt-4 cursor-pointer">
              Terms & Conditions
            </li>
            <li className="text-[13px] font-[400] pt-4 cursor-pointer">
              Privacy Policy
            </li>
          </ul>
        </div>
        <div className="w-[40%]">
          <p className="text-[15px] font-[400] text-[#94A3B8] cursor-pointer">
            NEWSLETTER
          </p>
          <div className="pt-3">
            <input
              type="email"
              placeholder="Enter your email address"
              className="w-[400px] h-[55px] border-[1px] border-[#E4E4E7] p-2 rounded-lg"
            />
            <button className="w-[400px] h-[55px] bg-[#FF6B01] mt-3 text-[white] rounded-lg cursor-pointer">
              Subscribe Now
            </button>
          </div>
        </div>
      </div>
      <div>
        <p className="h-[2px] w-[90%] bg-[#E2E8F0] mx-auto mt-6"></p>
        <p className="py-8 text-center text-[13px] font-[400] text-[#090914]">© Copyright 2024, All Rights Reserved by Mohan UIX</p>
      </div>
    </>
  );
};

export default Footer;