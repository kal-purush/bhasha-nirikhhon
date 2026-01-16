import * as React from 'react';
import { BrowserView, MobileView, isBrowser, isMobile } from 'react-device-detect';
import Pagination from '@mui/material/Pagination';
import Stack from '@mui/material/Stack';

export default function BasicPagination({ productsPerPage, totalProducts, paginate }) {

  const pageNumbers = [];
  for (let i = 1; i <= Math.ceil(totalProducts / productsPerPage); i++) {
    pageNumbers.push(i);
  }
  let size = ''
  if (isMobile) {
    size = 'small'
  }

  return (
    <Stack spacing={2}>
      <Pagination 
        onClick={paginate}
        count={pageNumbers.length} 
        color="primary"
        size={size}
      />
    </Stack>
  );
}