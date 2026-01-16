import styled from 'styled-components';
import { space, color, typography } from 'styled-system';
import Text from 'components/shared/text';

const IconLink = styled(Text)(color, space, typography);

IconLink.defaultProps = {
  color: 'Grays.100',
  textDecoration: 'underline',
  fontSize: 3,
  ml: 3,
};

export { IconLink };