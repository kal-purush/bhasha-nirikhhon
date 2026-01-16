import React from 'react';

function Forgot() {

  return (
    <div className="w-full max-w-xs my-32 mx-auto">
      <form className="bg-white shadow-xl rounded px-8 pt-6 pb-8 mb-4">
        <div className="mb-4">
            <h1 className='text-center text-xl font-bold mb-8 mt-8'>Recovery your account</h1>
          <label htmlFor="username" className="block text-gray-700 text-sm font-bold mb-2">
            Your email
          </label>
          <input
            type="text"
            className={'shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline'}
            placeholder="name@gamil.com"
          />
        </div>
        
        <div className="flex items-center justify-between">
          <button
            className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded focus:outline-none focus:shadow-outline"
            type="button"
          >
            Recovery
          </button>
        </div>
      </form>
      <p className="text-center text-gray-500 text-xs">&copy; 2023 Shopping Center!</p>
    </div>
  );
}

export default Forgot;