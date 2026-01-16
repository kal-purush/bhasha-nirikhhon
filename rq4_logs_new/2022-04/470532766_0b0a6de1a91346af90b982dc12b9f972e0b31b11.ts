import { expect } from 'chai';
import Button from './Button';
import { ButtonProps } from './Button.types';

describe('Button', () => {
  it('Рендер компонента Button', () => {
    const props: ButtonProps = {
      buttonText: 'текст кнопки',
      customClass: 'класс кнопки',
    };
    const button = new Button(props);

    expect(button.props.buttonText).to.be.equal(props.buttonText);
    expect(button.props.customClass).to.be.equal(props.customClass);
  });
});