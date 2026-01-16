import Navbar from "@/components/navbar";
import Image from "next/image";

export default function HobbyPage () {
    const hobbys = [
        {
            name: 'เล่นเกมส์',
            image: 'https://www.blognone.com/sites/default/files/externals/7a81ef81cc5cc68319aebbd75a6fb13d.jpeg'
        },
        {
            name: 'อ่านหนังสือ',
            image: 'https://storage.googleapis.com/techsauce-prod/ugc/uploads/2021/4/read_a_book_1200x800_Template_logo_black.jpg'
        },
        {
            name: 'ฟังเพลง',
            image: 'https://www.ofm.co.th/blog/wp-content/uploads/2020/10/%E0%B9%80%E0%B8%9E%E0%B8%A5%E0%B8%87%E0%B8%81%E0%B8%B1%E0%B8%9A%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%97%E0%B8%B3%E0%B8%87%E0%B8%B2%E0%B8%99_3.jpg'
        },
        {
            name: 'ทำอาหาร',
            image: 'https://d3h1lg3ksw6i6b.cloudfront.net/media/image/2021/05/28/12ebd12ac4c74bf8b55c3156548d1bed_What+Do+MICHELIN+Restaurant+Chefs+Cook+At+Home%3F1.jpg'
        },
        {
            name: 'เลี้ยงแมว',
            image: 'https://www.bolttech.co.th/blog/wp-content/uploads/2021/12/1-%E0%B9%80%E0%B8%A5%E0%B8%B5%E0%B9%89%E0%B8%A2%E0%B8%87%E0%B9%81%E0%B8%A1%E0%B8%A7%E0%B8%AA%E0%B8%B1%E0%B8%81%E0%B8%95%E0%B8%B1%E0%B8%A7-%E0%B8%A1%E0%B8%B5%E0%B8%84%E0%B9%88%E0%B8%B2%E0%B9%83%E0%B8%8A%E0%B9%89%E0%B8%88%E0%B9%88%E0%B8%B2%E0%B8%A2%E0%B8%AD%E0%B8%B0%E0%B9%84%E0%B8%A3%E0%B8%9A%E0%B9%89%E0%B8%B2%E0%B8%87.jpg'
        },
    ]
    return (
    <>
        <Navbar />
        <div className="w-3/4 m-auto space-y-4">
            <h1 className="text-2xl font-bold italic underline">Hobby</h1>
            <div className="grid grid-cols-4 gap-4">
                {
                    hobbys.map((hobby, index) => (
                        <div key={index} className="bg-slate-300 w-fit p-6 rounded-lg text-center flex justify-between flex-col h-[220px]">
                            <Image src={hobby.image} alt={hobby.name} width={200} height={200}className="rounded-md"/>
                            <span className="text-lg font-semibold">{hobby.name}</span>
                        </div>
                    ))
                }
            </div>
        </div>
    </>
    )
}