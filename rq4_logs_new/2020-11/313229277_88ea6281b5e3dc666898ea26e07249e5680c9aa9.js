import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import Form from 'components/Form';

import injectSaga from 'utils/injectSaga';
import { changeFormData } from 'core/Form/actions';
import saga from 'core/Form/saga';
import { getLoadingStatus, getFailedStatus } from 'core/Form/selectors';

const mapStateToProps = createStructuredSelector({
  isSuccess: getLoadingStatus(),
  isFailed: getFailedStatus(),
});

const mapDispatchToProps = (dispatch) => ({
  changeForm: ({ data }) => dispatch(changeFormData({ data })),
});

const withConnect = connect(mapStateToProps, mapDispatchToProps);
const withSaga = injectSaga({ key: 'form', saga });

export default compose(withSaga, withConnect)(Form);