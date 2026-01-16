import React from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/extend-expect";
import { Pagnation } from "../components/ui";

describe("Pagnation", () => {
  test("Pagnation 컴포넌트가 정상적으로 렌더링 되는지 확인", () => {
    render(<Pagnation totalPages={6} itemPerPages={3} />);

    const prevButton = screen.getByTestId("prev-button");
    const nextButton = screen.getByTestId("next-button");

    const pageNumbers = screen.getAllByTestId("page-number");

    pageNumbers.forEach((page, i) => {
      expect(page).toHaveTextContent(`${i + 1}`);
    });

    expect(prevButton).toBeInTheDocument();
    expect(nextButton).toBeInTheDocument();
  });

  test("Pagnation 첫번째 페이지에서 이전 페이지가 갈 수 없는 지 확인", () => {
    render(<Pagnation totalPages={6} itemPerPages={3} />);

    const prevButton = screen.getByTestId("prev-button");
    const nextButton = screen.getByTestId("next-button");

    fireEvent.click(prevButton);

    expect(prevButton).toHaveClass("disabled");
    expect(nextButton).not.toHaveClass("disabled");
  });

  test("Pagnation 마지막 페이지에서 다음 페이지로 갈 수 없는 지 확인", () => {
    render(<Pagnation totalPages={6} itemPerPages={3} />);

    const prevButton = screen.getByTestId("prev-button");
    const nextButton = screen.getByTestId("next-button");

    fireEvent.click(nextButton);

    expect(prevButton).not.toHaveClass("disabled");
    expect(nextButton).toHaveClass("disabled");
  });

  test("Pagnation 중간 페이지를 눌렀을 때 해당 페이지로 이동하는지 확인", () => {
    render(<Pagnation totalPages={6} itemPerPages={2} />);

    const prevButton = screen.getByTestId("prev-button");
    const nextButton = screen.getByTestId("next-button");

    fireEvent.click(nextButton);

    expect(prevButton).not.toHaveClass("disabled");
    expect(nextButton).not.toHaveClass("disabled");
  });
});