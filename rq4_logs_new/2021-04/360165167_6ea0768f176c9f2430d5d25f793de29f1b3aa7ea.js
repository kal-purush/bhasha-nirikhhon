import React from 'react';
import { TouchableWithoutFeedback, Image } from 'react-native';
import {
  Header,
  Left,
  Button,
  Icon,
  Body,
  Right,
  Title,
} from 'native-base';
import { PropTypes } from 'prop-types';

import styles from './styles';

const logo = require('../../../assets/images/darumaLogo.png');

class ActionHeader extends React.PureComponent {
  renderLeft = () => {
    const {
      leftAction,
      leftIcon,
      showLogo,
      iconType,
      transparent,
      customleftIconStyle,
    } = this.props;

    if (leftAction || showLogo) {
      return (
        <Left
          style={styles.leftStyle}
        >
          {leftAction && (
            <Button
              transparent
              onPress={leftAction}
            >
              <Icon
                name={leftIcon}
                type={iconType}
                style={[
                  styles.leftIcon,
                  transparent && styles.inverseLeftIcon,
                  customleftIconStyle,
                ]}
              />
            </Button>
          )}
          {showLogo && (
            <Image
              source={logo}
              style={styles.imageLogo}
            />
          )}
        </Left>
      );
    }

    return null;
  }

  render() {
    const {
      title,
      children,
      noShadow,
      transparent,
      bold,
      customStyle,
      onTitlePress,
      customTitleStyle,
    } = this.props;
    const left = this.renderLeft();
    const right = (React.Children.count(children) > 0)
      ? (
        <Right
          style={styles.rightStyle}
        >
          {children}
        </Right>
      ) : <Right style={styles.rightStyle} />;
    return (
      <Header
        transparent={transparent}
        noShadow={noShadow}
        style={[
          customStyle,
          !noShadow ? styles.shadow : null,
        ]}
      >
        {left}
        <Body style={styles.bodyStyle}>
          <TouchableWithoutFeedback onPress={onTitlePress}>
            <Title
              adjustsFontSizeToFit
              ref={(el) => { if (title) this.viewRef = el; }}
              style={[
                styles.titleStyle,
                bold && styles.bold,
                customTitleStyle,
              ]}
            >
              {title}
            </Title>
          </TouchableWithoutFeedback>
        </Body>
        {right}
      </Header>
    );
  }
}

ActionHeader.propTypes = {
  children: PropTypes.node,
  title: PropTypes.string,
  leftAction: PropTypes.func,
  leftIcon: PropTypes.string,
  showLogo: PropTypes.bool,
  noShadow: PropTypes.bool,
  iconType: PropTypes.string,
  transparent: PropTypes.bool,
  bold: PropTypes.bool,
  customStyle: Header.propTypes.style,
  onTitlePress: PropTypes.func,
  customTitleStyle: Title.propTypes.style,
  customleftIconStyle: Icon.propTypes.style,
};

ActionHeader.defaultProps = {
  children: null,
  title: null,
  leftAction: undefined,
  leftIcon: 'arrow-back',
  showLogo: false,
  iconType: 'MaterialIcons',
  noShadow: true,
  transparent: false,
  bold: false,
  customStyle: undefined,
  onTitlePress: null,
  customTitleStyle: null,
  customleftIconStyle: null,
};

export default ActionHeader;