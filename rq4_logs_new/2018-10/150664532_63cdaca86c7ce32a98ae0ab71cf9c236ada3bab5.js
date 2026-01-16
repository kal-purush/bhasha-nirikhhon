import React from 'react';
import PropTypes from 'prop-types';

const Text = (props) => {
    const { textRenderer, texts } = props;
    return (
        <span>
      {textRenderer(...texts)}
            {' '}
    </span>
    );
};

Text.propTypes = {
    texts: PropTypes.arrayOf(PropTypes.string.isRequired),
    textRenderer: PropTypes.func.isRequired,
};

export default Text;