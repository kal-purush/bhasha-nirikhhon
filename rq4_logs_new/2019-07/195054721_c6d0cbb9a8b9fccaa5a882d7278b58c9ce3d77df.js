import React, { useCallback } from 'react';
import styled from 'styled-components';
import _ from 'loadsh';
import useFetch from '../common/useFetch';
import Person from './person';
import { data } from '../../utils/mockData';

const S = {};
S.Teams = styled('div')`
  display: flex;
  flex-wrap: wrap;
  margin: -12px;
`;
const Teams = React.memo((props) => {
  // const { data, isLoading, error, abort } = useFetch('');
  const renderPerson = useCallback(() => {
    return _.values(data).map((item, index) => {
      return <Person 
                key={index}
                imgSrc={item.avatar[0].url}
                name={item.fullName}
                title={item.title}
                focusTopic={item.focusOn.join(', ')}
                email={item.email}
                website={item.website}
                publication={item.publication}
                {...props}
              />
    })
  },[data]);

  return (
    <S.Teams>
      {renderPerson()}
    </S.Teams>
  );
});

export default Teams;