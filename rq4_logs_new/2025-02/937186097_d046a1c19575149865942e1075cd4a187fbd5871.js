import { updateOrder } from "../../data/order.data";
import loginPage from "../../pages/login.page";
import orderFormPage from "../../pages/sales/order-form.page";
import orderPage from "../../pages/sales/order.page";

describe("Edit Pesanan", () => {
  beforeEach(() => {
    const email = Cypress.env("email");
    const password = Cypress.env("password");

    loginPage.login(email, password);
    orderPage.visitOrderPage();
  });

  it("Search order by no pesanan", () => {
    const noPesanan = "SO-000000003";
    orderPage.searchOrderByText(noPesanan);
    cy.contains(noPesanan).should("exist");
  });

  it("Search order by Status Jubelio - Lunas", () => {
    const status = "Lunas";
    orderPage.searchOrderByStatus("Pilih Status Jubelio", status);
    cy.contains(status).should("exist");
  });

  it("Search order by Status Jubelio - Pending", () => {
    const status = "PENDING";
    orderPage.searchOrderByStatus("Pilih Status Jubelio", status);
    cy.contains(status).should("exist");
  });

  it("Search order by Sumber - Bukalapak", () => {
    const sumber = "BUKALAPAK";
    orderPage.searchOrderByStatus("Pilih Sumber", sumber);
    cy.contains(sumber).should("exist");
  });

  it("Search order by Sumber - Blibli", () => {
    const sumber = "BLIBLI";
    orderPage.searchOrderByStatus("Pilih Sumber", sumber);
    cy.contains(sumber).should("exist");
  });
});