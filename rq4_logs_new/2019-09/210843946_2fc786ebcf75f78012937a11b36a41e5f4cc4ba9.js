import React from "react";
import { MemoryRouter } from "react-router-dom";
import { render, unmountComponentAtNode } from "react-dom";
import { act } from "react-dom/test-utils";

import Header from './../components/Header/Header';

let container = null;
beforeEach(() => {
  // setup a DOM element as a render target
  container = document.createElement("div");
  document.body.appendChild(container);
});

afterEach(() => {
  // cleanup on exiting
  unmountComponentAtNode(container);
  container.remove();
  container = null;
});

it("renders with main view link", () => {
  act(() => {
    render(
      <MemoryRouter>
        <Header />
      </MemoryRouter>, 
      container
    );
  });
  const title = container.querySelector('.header-section a p');
  expect(title.textContent).toBe('Main');
});

it("renders with stream view link", () => {
  act(() => {
    render(
      <MemoryRouter>
        <Header />
      </MemoryRouter>, 
      container
    );
  });
  const title = container.querySelector('.header-section a:nth-child(2) p');
  expect(title.textContent).toBe('Stream');
});