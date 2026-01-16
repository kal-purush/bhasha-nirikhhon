import React from "react";
import user from "../assets/profile.png";

const Layout = ({ children }) => {
  return (
    <div className="flex h-screen">
      {/* Sider */}
      <div className="bg-white w-16 border-r"></div>

      {/* Main Content */}
      <div className="flex flex-col flex-1">
        {/* Header */}
        <header className="bg-white border-b px-10 py-4 flex justify-between items-center">
          <h1 className="text-lg font-bold text-gray-700"> </h1>
          <div className="flex items-center gap-2">
            <span className="text-yellow-500 font-semibold">Hi,</span>
            <span className="text-green-600 font-semibold">User!</span>
            <div className="w-8 h-8 flex items-center justify-center">
                <img src={user} alt="Profile" className="w-full h-full object-cover" />
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="flex-1 bg-gray-200 overflow-y-auto p-6">{children}</main>

        {/* Footer */}
        <footer className="bg-white border-t px-10 py-4 text-center text-gray-600">
          © 2025 PC Persis Banjaran
        </footer>
      </div>
    </div>
  );
};

export default Layout;