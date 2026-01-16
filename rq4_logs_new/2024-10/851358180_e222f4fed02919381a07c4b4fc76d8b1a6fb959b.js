import React from 'react';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom/extend-expect';
import RecordList from './RecordList';

describe('RecordList Component', () => {
  const mockRecords = [
    { id: 1, name: 'Record 1' },
    { id: 2, name: 'Record 2' },
  ];

  it('renders the list of records', () => {
    render(<RecordList records={mockRecords} />);
    const recordItems = screen.getAllByRole('listitem');
    expect(recordItems).toHaveLength(2);
    expect(recordItems[0]).toHaveTextContent('Record 1');
    expect(recordItems[1]).toHaveTextContent('Record 2');
  });
});