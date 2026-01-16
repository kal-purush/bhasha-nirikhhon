import React from 'react';
import Link from 'next/link';
import { motion } from 'framer-motion';

const MotionLink = motion(Link);

const Logo = () => {
    return (
        <div className='flex items-center justify-center'>
            <MotionLink 
                href="/"  // Valid href prop for the Link component
                className='w-16 h-16 bg-dark text-light flex items-center justify-center rounded-full text-xs font-semibold'  // Ensure circle shape and text size
                whileHover={{
                    scale: 1.2,
                    transition: {
                        duration: 0.3,  // Smooth transition effect
                    }
                }}
            >
                <span className='text-xs'>KaitCo</span>  {/* Text size adjustment */}
            </MotionLink>
        </div>
    );
};

export default Logo;