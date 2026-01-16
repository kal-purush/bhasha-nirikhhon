import React from 'react';

const Loader = () => {
    let circleCommonClasses = 'h-2.5 w-2.5 bg-current rounded-full bg-blue600 ';

    return (
        <div className="flex justify-center mt-16">
            <div className={`${circleCommonClasses} mr-1 animate-bounce`} />
            <div className={`${circleCommonClasses} mr-1 animate-bounce200`} />
            <div className={`${circleCommonClasses} animate-bounce400`} />
        </div>
    );
};

export default Loader;