import React, { useState } from 'react';

const ToolTip = ({ text, children }) => {
    const [showTooltip, setShowTooltip] = useState(false);

    const handleMouseEnter = () => {
        setShowTooltip(true);
    };

    const handleMouseLeave = () => {
        setShowTooltip(false);
    };

    return (
        <div className="relative inline-block group">
            <div
                onMouseEnter={handleMouseEnter}
                onMouseLeave={handleMouseLeave}
            >
                {children}
            </div>
            {showTooltip && (
                <div className="absolute opacity-0 group-hover:opacity-100 bg-gray-800 text-white text-sm px-2 py-1 rounded-md mt-2 transition-opacity duration-300">
                    {text}
                </div>
            )}
        </div>
    );
};

export default ToolTip;