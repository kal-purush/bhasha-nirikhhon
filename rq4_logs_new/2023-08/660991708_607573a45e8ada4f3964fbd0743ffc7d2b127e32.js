import '@testing-library/jest-dom';
import { fireEvent, render } from '../../lib/redux/reduxRender';
import SearchBar from '.';

describe('Test SearchBar component', () => {
    test('Should change the entry of the input', () => {
        const { getByPlaceholderText } = render(<SearchBar />);
        const input = getByPlaceholderText('Rechercher');
        fireEvent.change(input, { target: { value: 'Marvel' } });
        expect(input.value).toBe('Marvel');
    });
});