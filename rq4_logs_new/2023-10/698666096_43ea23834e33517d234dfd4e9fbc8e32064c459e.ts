import {Report} from "@/type/Report";
import {createReportGeneralSection} from './createReportGeneralSection'
import {createReportCaseSection} from './createReportCaseSection'
import {createReportSummarySection} from './createReportSummarySection'

export function createReportBody(config: Report, usdPlnExchangeTime: string) {
  const body = document.createElement('main');
  const generalSection = createReportGeneralSection(config);
  const caseSection = createReportCaseSection(config);
  const summarySection = createReportSummarySection(config, usdPlnExchangeTime);
  body.appendChild(generalSection);
  body.appendChild(caseSection);
  body.appendChild(summarySection);
  return body;
}