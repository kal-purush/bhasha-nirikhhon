/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        "./index.html", // আপনার HTML ফাইলের পাথ
            "./src/**/*.{html,js}", // আপনার src ফোল্ডারের সমস্ত HTML এবং JS ফাইল
              ],
                theme: {
                    extend: {}, // আপনি কাস্টমাইজেশন এখানে যোগ করতে পারেন
                      },
                        plugins: [], // যদি কোনো Tailwind প্লাগিন ব্যবহার করেন
                        }
  