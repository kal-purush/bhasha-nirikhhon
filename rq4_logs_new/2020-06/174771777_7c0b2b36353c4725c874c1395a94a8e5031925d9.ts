import { Component, OnInit } from "@angular/core";
import { CarteraProvision } from "../../../crosscommon/entities/CarteraProvision";
import {
  PaymentReportService,
  iPaymentReportData
} from "./PaymentReportService";
import { CarteraPayment } from "src/crosscommon/entities/CarteraPayment";
import { DateUtils } from "src/crosscommon/DateUtility";
import { Title } from "@angular/platform-browser";
import { CarteraPayDet } from "src/crosscommon/entities/CarteraPayDet";

const UNIT_LABEL = "Departamento";

@Component({
  selector: "payment-report",
  templateUrl: "./PaymentReport.html",
  styleUrls: ["./PaymentReport.css"],
  providers: [PaymentReportService]
})
export class PaymentReportComponent implements OnInit {
  public viewData: {
    paymentReportData: iPaymentReportData[];
    title: string;
    year: number;
    month: number;
    displayYearMonth: string;
    previousTotal: number;
    total: number;
  } = {
    paymentReportData: [],
    title: null,
    year: 0,
    month: 0,
    displayYearMonth: null,
    previousTotal: 0,
    total: 0
  };

  parseQueryString() {
    const query = this.paymentReportService.getQueryStringParameters();

    this.viewData.year = parseInt(query.get("year"), 10);
    this.viewData.month = parseInt(query.get("month"), 10);

    this.viewData.displayYearMonth = `${DateUtils.getMonthNameSpanish(
      this.viewData.month
    )} ${this.viewData.year}`;
  }

  constructor(
    private paymentReportService: PaymentReportService,
    private titleService: Title
  ) {
    this.parseQueryString();
    this.viewData.title = `Recaudación ${this.viewData.displayYearMonth} FFJ78`;
    titleService.setTitle(this.viewData.title);
  }

  ngOnInit() {
    this.deriveState();
  }

  sumByField<T>(list: T[], fieldName: string): number {
    return list.reduce(
      (previous: number, current: T) => previous + current[fieldName],
      0
    );
  }

  getUnitFromMatchHint(payment: CarteraPayment): number {
    return parseInt(payment.cpy_match_hint.substr(-3), 10);
  }

  deriveState() {
    this.viewData.previousTotal = 0;
    this.viewData.total = 0;

    this.paymentReportService
      .getPaymentsForMonth(this.viewData.year, this.viewData.month)
      .then((response: iPaymentReportData[]) => {
        this.viewData.paymentReportData = response;

        this.viewData.paymentReportData.forEach(item => {
          if (item.previousPayDetList.length) {
            item["previousPayDetTotal"] = item.previousPayDetList.reduce(
              (total, current) => total + current.cpd_amount,
              0
            );
          }

          // total sum for previuos payments
          this.viewData.previousTotal += item.previousPayDetList.reduce(
            (total, current) => total + current.cpd_amount,
            0
          );

          // total sum
          this.viewData.total += item.paymentList.reduce(
            (total, current) => total + current.payment.cpy_amount,
            0
          );
        });
      });
  }
}