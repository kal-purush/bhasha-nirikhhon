import propTypes from 'prop-types';
import React from 'react';
import Row from './row';
import { filterEvents } from '../../utils';

const Histogram = props => {
  const {
    className,
    emptyColor,
    fillColor,
    reviewCounts,
    star,
  } = props;

  let totalReviews = 0;
  for (let i = 0; i < reviewCounts.length; i++) {
    totalReviews += reviewCounts[i].count;
  }

  const rowProps = {
    totalReviews,
    emptyColor,
    fillColor,
    star,
  };

  const rows = [];
  for (let i = 0; i < reviewCounts.length; i++) {
    rows.push(
      <Row
        rating={reviewCounts[i].rating}
        reviewCount={reviewCounts[i].count}
        {...rowProps}
        {...filterEvents(props)}
        key={i}
      />
    );
  }

  return (
    <div className={className}>{rows}</div>
  );
};

Histogram.displayName = 'Histogram';

Histogram.defaultProps = {
  emptyColor: '#ccc',
  fillColor: '#0066b4',
};

Histogram.propTypes = {
    className: propTypes.string,
    emptyColor: propTypes.string,
    fillColor: propTypes.string,
    reviewCounts: propTypes.array.isRequired,
    star: propTypes.element.isRequired,
};

export default Histogram;