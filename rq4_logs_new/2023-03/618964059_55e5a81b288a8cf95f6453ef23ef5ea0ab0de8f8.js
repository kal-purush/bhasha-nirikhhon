import * as React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';





function VentRemoteContent() {

  return (
      <Box sx={{ display: 'flex' }}>
            <Typography
              component="h1"
              variant="h6"
              color="inherit"
              noWrap
              sx={{ flexGrow: 1 }}
            >
                Tunnel Name
            </Typography>
        </Box>
  );
}

export default function VentRemote() {
  return <VentRemoteContent />;
}