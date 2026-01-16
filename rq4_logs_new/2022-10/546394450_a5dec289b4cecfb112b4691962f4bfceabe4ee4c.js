import * as React from 'react';
import Box from '@mui/material/Box';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Typography from '@mui/material/Typography';


const card = (
  <React.Fragment>
    <CardContent>
      <Typography variant="h5" component="div">
        DMV
      </Typography>
      <Typography sx={{ mb: 1.5 }} color="text.secondary">
        Test User
      </Typography>
      <Typography variant="body2">
         <p>
        Test User Info listed here
        </p>
        <p>
        Test User Info listed here
        </p>
        <p>
        Test User Info listed here
        </p>
        <p>
        Test User Info listed here
        </p>
        <p>
        Test User Info listed here
        </p>
        <p>
        Test User Info listed here
        </p>
        <p>
        Test User Info listed here
        </p>
        <p>
        Test User Info listed here
        </p>
        <br />
      </Typography>
    </CardContent>
  </React.Fragment>
);

export default function OutlinedCard() {
  return (
    <Box sx={{ minWidth: 275 }}>
      <Card variant="outlined">{card}</Card>
    </Box>
  );
}