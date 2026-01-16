import React from 'react';
import PropTypes from 'prop-types';

import {
  connect,
} from 'react-redux';

import {
  loadValues,
  saveValues,
} from '../../modules/storage';

import {
  disableRevertButton,
  enableRevertButton,
  disableSaveButton,
  enableSaveButton,
} from '../../redux/button';

import {
  update as updateConfig,
} from '../../redux/config';

import Presentation from './presentation';

function mapStateToProps(state) {
  const {
    button,
    config,
  } = state;

  return {
    ...button,
    ...config,
  };
}

function mapDispatchToProps(dispatch) {
  return {
    /**
     * handler for update configs
     *
     * @param {Object} params
     */
    onChangeConfig(params) {
      dispatch(updateConfig(params));
    },
    /**
     * handler for click revert button
     */
    async onClickRevert() {
      dispatch(disableRevertButton());

      try {
        const values = await loadValues(),
              keys = Object.keys(values);

        keys.forEach(
          (key) => dispatch(updateConfig({
            name: key,
            value: values[key],
          }))
        );
      } catch(e) {
        console.error(e);
      }

      dispatch(enableRevertButton());
    },
    /**
     * save configs to local storage
     *
     * @param {Object} params
     */
    async saveConfigs(params) {
      dispatch(disableSaveButton());

      try {
        const {
          account,
          branch,
          path,
          repository,
          template,
        } = params;

        await saveValues({
          account,
          branch,
          path,
          repository,
          template,
        });
      } catch(e) {
        console.error(e.stack || e);
      }

      dispatch(enableSaveButton());
    },
  };
}

function mergeProps(stateProps, dispatchProps, ownProps) {
  return Object.assign({}, ownProps, stateProps, dispatchProps, {
    /**
     * handler for click save button
     */
    onClickSave() {
      // NOTE: saveConfigs need state
      dispatchProps.saveConfigs(stateProps);
    },
  });
}

class Container extends React.Component {
  render() {
    return (
      <Presentation {...this.props} />
    );
  }
}

Container.propType = {};

export default connect(mapStateToProps, mapDispatchToProps, mergeProps)(Container);