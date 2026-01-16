import { render, screen } from "@testing-library/svelte";
import { test, expect, vi } from "vitest";
import App from "./App.svelte";
import browser from "webextension-polyfill";

test("the app renders", () => {
  vi.mocked(browser.storage.local.get).mockReturnValue(Promise.resolve({
    enterpriseHosts: undefined,
  }));

  vi.mocked(browser.tabs.query).mockReturnValue(
    Promise.resolve([{
      index: 0,
      active: true,
      highlighted: true,
      pinned: false,
      incognito: false,
      url: "https://github.com/Davidonium/test-project/pulls/1234"
    }])
  );

  render(App);

  const copyBtn = screen.getByRole("button", { name: "Copy" });

  expect(copyBtn).toBeInTheDocument();
});