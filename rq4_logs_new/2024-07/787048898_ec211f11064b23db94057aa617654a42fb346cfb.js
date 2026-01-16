import React from 'react';
import { Box } from '@mui/material';

const BlurredName = ({ text }) => {
    return (
        <Box
            sx={{
                filter: 'blur(5px)',
                WebkitFilter: 'blur(5px)', // For Safari compatibility
                width: '100px',
                height: '20px',
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                backgroundColor: '#ddd',
                borderRadius: '4px',
                color: 'transparent', // Make text transparent
            }}
        >
            {text}
        </Box>
    );
};

export default BlurredName;