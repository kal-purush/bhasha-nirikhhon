import Image from 'next/image';

export default function Dashboard() {
  return (
    <section className="py-16">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-8 items-center">
        <div className="relative h-96">
          <Image
            src="/2.png"
            alt="Dashboard preview"
            layout="fill"
            objectFit="contain"
            className='h-full rounded-xl'
          />
        </div>
        <div>
          <h2 className="text-3xl font-bold mb-4">Demo our dashboard</h2>
          <p className="mb-6">
            Explore a multitude of benefits meticulously tailored to meet the unique needs of buyers.
          </p>
          <div className="bg-green-500 text-white p-4 rounded-lg inline-block">
            <span className="text-4xl font-bold">40%</span>
            <p>JET AI Chatbot slashes response times</p>
          </div>
        </div>
      </div>
    </section>
  );
}