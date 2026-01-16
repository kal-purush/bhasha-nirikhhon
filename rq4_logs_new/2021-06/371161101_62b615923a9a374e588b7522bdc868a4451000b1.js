import React from 'react';
import { Heading, Link } from '@chakra-ui/react';
import { getUserProviderLink } from '@/utils/provider';

export const FeedbackAuthor = ({ color, authorName, email, provider }) => {
  const authorLink = getUserProviderLink(provider, authorName, email);

  return (
    <Link fontWeight="bold" fontSize="sm" href={authorLink} target={'_blank'}>
      <Heading size="sm" as="h3" mb={0} fontWeight="medium" color={color}>
        {authorName}
      </Heading>
    </Link>
  );
};