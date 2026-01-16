/*
 * This program has been developed by students from the bachelor
 * Computer Science at Utrecht University within the Software Project course.
 *
 * © Copyright Utrecht University
 * (Department of Information and Computing Sciences)
 */

import { http, HttpResponse, passthrough } from "msw";
import { addonList } from "../temp/tempAddons";
import { AddonCategory } from "../types/AddOnTypes";

const baseUrl = import.meta.env.VITE_API_BASE;

/**
 * Define mocking routes.
 * Roughly corresponds to backend handlers.
 */
export const handlers = [
  http.post(`${baseUrl}/addons/get`, async ({ request }) => {
    const body = (await request.json()) as {
      page?: number;
      category?: AddonCategory;
    };
    const category = body.category;

    let filteredAddons = category
      ? addonList.filter(addon => addon.category === category)
      : addonList;

    return HttpResponse.json({ addons: filteredAddons });
  }),

  http.post(`${baseUrl}/addons/get-by-id`, async ({ request }) => {
    const body = (await request.json()) as {
      id: string;
    };
    const addonId = body.id;

    const addon = addonList.find(addon => addon.id === addonId);

    return addon
      ? HttpResponse.json({ addon: addon })
      : HttpResponse.json(null);
  }),

  http.post(`${baseUrl}/addons/get-readme`, async ({ request }) => {
    const body = (await request.json()) as {
      id: string;
    };
    const addonId = body.id;

    const addon = addonList.find(addon => addon.id === addonId);

    return addon
      ? HttpResponse.json({ readme: `# README for ${addon.name}` })
      : HttpResponse.json(null);
  }),
  http.all("*", () => {
    return passthrough();
  })
];