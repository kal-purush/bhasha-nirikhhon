import { Test } from "@nestjs/testing";
import axios from "axios";
import mockAxios from "jest-mock-axios";

import { CampayModule } from "../campay.module";
import { CampayService } from "../campay.service";
import { CampayCurrency } from "../campay.types";
import { CampayHttpClientService } from "../campay-http-client.service";

const moduleFixture = Test.createTestingModule({
  imports: [
    CampayModule.forRoot({
      apiKey: "api-key-xxx-yyy"
    })
  ],
  providers: []
});

describe("Campay service", () => {
  let campayService: CampayService;
  let campayHttpClientService: CampayHttpClientService;

  beforeEach(async () => {
    const module = await moduleFixture.compile();
    campayService = module.get(CampayService);
    campayHttpClientService = module.get(CampayHttpClientService);

    axios.post = jest.fn().mockResolvedValue({ ok: true });
    axios.get = jest.fn().mockResolvedValue({ ok: true });

    expect(
      campayHttpClientService.getAxiosInstance().defaults.headers.common
        .Authorization
    ).toBe("Token api-key-xxx-yyy");

    console.log("👔", campayHttpClientService.getAxiosInstance().interceptors);
  });

  it("should correctly query history", async () => {
    await campayService.getHistory({
      start_date: "2024-03-02",
      end_date: "2024-03-05"
    });
    expect(
      campayHttpClientService.getAxiosInstance().post
    ).toHaveBeenCalledWith(
      "https://demo.campay.net/api/history/",
      {
        start_date: "2024-03-02",
        end_date: "2024-03-05"
      },
      { headers: undefined }
    );
  });

  it("should correctly collect funds", async () => {
    await campayService.collect({
      amount: 100,
      from: "237677777777",
      description: "testing",
      uuid: "uuid-1234"
    });
    expect(
      campayHttpClientService.getAxiosInstance().post
    ).toHaveBeenCalledWith(
      "https://demo.campay.net/api/collect/",
      {
        amount: 100,
        from: "237677777777",
        description: "testing",
        uuid: "uuid-1234",
        currency: CampayCurrency.Cfa
      },
      { headers: undefined }
    );
  });

  it("should correctly get balance", async () => {
    await campayService.getBalance();

    expect(campayHttpClientService.getAxiosInstance().get).toHaveBeenCalledWith(
      "https://demo.campay.net/api/balance/",
      {}
    );
  });

  it("should correctly transfer airtime", async () => {
    await campayService.airTimeTransfer({
      amount: 1000,
      to: "237677777777"
    });

    expect(
      campayHttpClientService.getAxiosInstance().post
    ).toHaveBeenCalledWith(
      "https://demo.campay.net/api/utilities/airtime/transfer/",
      {
        amount: 1000,
        to: "237677777777"
      },
      { headers: undefined }
    );
  });

  it("should correctly get payment link", async () => {
    await campayService.getPaymentLink({
      amount: 1000,
      description: "a payment by link",
      redirect_url: "https://my-secure-website.com/redirect",
      payment_options: "Card",
      failure_redirect_url: "https://my-secure-website.com/failure-redirect"
    });

    expect(
      campayHttpClientService.getAxiosInstance().post
    ).toHaveBeenCalledWith(
      "https://demo.campay.net/api/get_payment_link/",
      {
        amount: 1000,
        description: "a payment by link",
        redirect_url: "https://my-secure-website.com/redirect",
        payment_options: "Card",
        failure_redirect_url: "https://my-secure-website.com/failure-redirect",
        currency: CampayCurrency.Cfa
      },
      { headers: undefined }
    );
  });

  it("should correctly get status", async () => {
    await campayService.getStatus({
      reference: "op-ref-1234"
    });

    expect(campayHttpClientService.getAxiosInstance().get).toHaveBeenCalledWith(
      "https://demo.campay.net/api/transaction/op-ref-1234",
      {}
    );
  });

  it("should correctly withdraw money", async () => {
    await campayService.withdraw({
      amount: 1000,
      to: "237677777777",
      description: "Get some cash"
    });

    expect(
      campayHttpClientService.getAxiosInstance().post
    ).toHaveBeenCalledWith(
      "https://demo.campay.net/api/withdraw/",
      { amount: 1000, to: "237677777777", description: "Get some cash" },
      { headers: undefined }
    );
  });

  afterEach(() => {
    mockAxios.reset();
  });
});