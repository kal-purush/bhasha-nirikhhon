import Navbar from "../components/layout/NavBar";
import Sidebar from "../components/layout/Sidebar";
import RightSidebar from "../components/layout/RightSideBar";
import "./globals.css"; // Ensure Tailwind styles are applied

export const metadata = {
  title: "Reddit Clone",
  description: "A Reddit-like web application built with Next.js",
};

export default function Layout({ children }) {
  return (
    <html lang="en">
      <body className="bg-white min-h-screen">
        {/* Navbar fixed at the top */}
        <Navbar />

        {/* Main Layout */}
        <div className="flex justify-center pt-16 px-4">
          {/* Left Sidebar */}
          <aside className="hidden lg:block w-64 h-screen overflow-y-auto sticky top-16">
            <Sidebar />
          </aside>

          {/* Main Content (Post Feed) */}
          <main className="w-full max-w-2xl mx-auto">{children}</main>

          {/* Right Sidebar */}
          <aside className="hidden xl:block w-80 h-screen overflow-y-auto sticky top-16">
            <RightSidebar />
          </aside>
        </div>
      </body>
    </html>
  );
}