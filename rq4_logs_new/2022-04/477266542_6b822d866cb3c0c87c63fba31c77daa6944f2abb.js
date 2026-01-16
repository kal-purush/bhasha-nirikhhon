import ResultBox from './ResultBox';
import { render, screen, cleanup } from '@testing-library/react';
import '@testing-library/jest-dom/extend-expect';
import { formatAmountInCurrency } from './../../utils/formatAmountInCurrency';

  describe('Component ResultBox', () => {

    it('should render without crashing', () => {
  
      // render component
      render(<ResultBox from="PLN" to="USD" amount={100} />);
    });

    const testAmount = [
      { amount: 100},
      { amount: 20},
      { amount: 200},
      { amount: 345},
    ];
  
    for(const testObj of testAmount) {

      it('should render proper info about conversion when PLN -> USD', () => {
      
        render(<ResultBox from="PLN" to="USD" amount={testObj.amount} />);
        
        const output = screen.getByTestId('output');
        const formatTestAmount = formatAmountInCurrency(testObj.amount, 'PLN');
        const converttestAmount = formatAmountInCurrency((testObj.amount/3.5), 'USD' );

        expect(output).toHaveTextContent(`${formatTestAmount} = ${converttestAmount}`);

      });
      
      // unmount component
      cleanup()      
    }

    for(const testObj of testAmount) {

      it('should render proper info about conversion when USD -> PLN', () => {
      
        render(<ResultBox from="USD" to="PLN" amount={testObj.amount} />);
        
        const output = screen.getByTestId('output');
        const formatTestAmount = formatAmountInCurrency(testObj.amount, 'USD');
        const converttestAmount = formatAmountInCurrency((testObj.amount*3.5), 'PLN' );

        expect(output).toHaveTextContent(`${formatTestAmount} = ${converttestAmount}`);

      });
      
      // unmount component
      cleanup()      
    }

    const testCurrencies = [
      { currency: 'PLN', amount: 100},
      { currency: 'PLN', amount: 200},
      { currency: 'USD', amount: 100},
      { currency: 'USD', amount: 200}
    ]
    for(const testObj of testCurrencies) {

      it('should display the correct information when the same currency is selected', () => {
      
        render(<ResultBox from={testObj.currency} to={testObj.currency} amount={testObj.amount} />);
        
        const output = screen.getByTestId('output');
        const formatTestAmount = formatAmountInCurrency(testObj.amount, (testObj.currency));

        expect(output).toHaveTextContent(`${formatTestAmount} = ${formatTestAmount}`);

      });
      
      // unmount component
      cleanup()      
    }

    const testNegativeValue = [
      { from: 'PLN', to: 'USD', amount: -100},
      { from: 'PLN', to: 'USD', amount: -200},
      { from: 'USD', to: 'PLN', amount: -50},
      { from: 'USD', to: 'PLN', amount: -1}
    ]
    for(const testObj of testNegativeValue) {

      it('should display “Wrong value…” when a negative value is entered', () => {
      
        render(<ResultBox from={testObj.from} to={testObj.to} amount={testObj.amount} />);
        
        const output = screen.getByTestId('output');
        
        expect(output).toHaveTextContent('Wrong value…');

      });
      
      // unmount component
      cleanup()      
    }
  });