import React from "react";
import {render} from '@testing-library/react'
import {Pagination} from "./Pagination";

it('renders correctly', () => {
    const {queryByTestId} = render(<Pagination/>)
    expect(queryByTestId("pagination")).toBeTruthy()
})