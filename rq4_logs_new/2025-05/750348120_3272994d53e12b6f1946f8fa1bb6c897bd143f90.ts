import { injectable } from 'inversify';
import { SspDashboardManagementRepository } from '../ssp-dashboard-management-repository';
import Chainable = Cypress.Chainable;

@injectable()
export class B2bMpSspDashboardManagementRepository implements SspDashboardManagementRepository {
  getUserInfoBlock(): Chainable {
    return cy.get('[data-qa="user-info"]');
  }
  getUserInfoBlockWelcome(): Chainable {
    return cy.get('[data-qa="user-info"] strong');
  }
  getWelcomeBlock(): Chainable {
    return cy.get('[data-qa="welcome-block"]');
  }
  getOverviewBlock(): Chainable {
    return cy.get('[data-qa="component stats-overview"]');
  }
  getOverviewTitle(): string {
    return 'My Overview';
  }
  getStatsColumnBlocks(): Chainable {
    return cy.get('[data-qa="stats-column"]');
  }
  getStatsColumnTitleName(): string {
    return '[data-qa="stats-column-title"]';
  }
  getStatsColumnCounterName(): string {
    return '[data-qa="stats-column-counter"]';
  }
  getSalesRepresentativeBlocks(): Chainable {
    return cy.get('[data-qa="sales-representative"]');
  }
  getAssetsBlock(): Chainable {
    return cy.get('[data-qa="component assets-preview"]');
  }
  getAssetPreviewBlock(): Chainable {
    return cy.get('[data-qa="component assets-preview"]').find('[data-qa="asset-item-preview"]');
  }
  getAssetPreviewItemBlock(index: number): Chainable {
    return cy
      .get('[data-qa="component assets-preview"]')
      .find('[data-qa="asset-item-preview"]:nth-child(' + (index + 1) + ')');
  }
  getAssetPreviewItemLinkBlock(index: number): Chainable {
    return cy
      .get('[data-qa="component assets-preview"]')
      .find('[data-qa="asset-item-preview"]:nth-child(' + (index + 1) + ')')
      .find('a.assets-preview__link');
  }
  getExpectedStatsColumnBlocks(): string[] {
    return ['Assets', 'Pending Inquiries'];
  }
  getPlaceholderImage(): string {
    return 'image-placeholder.png';
  }
  getFilesBlock(): Chainable {
    return cy.get('[data-qa="component dashboard-table dashboard-table-table-files"]');
  }
  getFilesBlockTitle(): Chainable {
    return cy
      .get('[data-qa="component dashboard-table dashboard-table-table-files"]')
      .find('[data-qa="dashboard-table-table-files-counter-title"]');
  }
  getNoFilesText(): string {
    return 'No Files';
  }
  getFilesHeaders(): string[] {
    return ['File Name', 'Size', 'File Type', ''];
  }
  getInquiriesBlock(): Chainable {
    return cy.get('[data-qa="component dashboard-table ssp-inquiry-table"]');
  }
  getInquiriesBlockTitle(): Chainable {
    return cy
      .get('[data-qa="component dashboard-table ssp-inquiry-table"]')
      .find('[data-qa="ssp-inquiry-table-counter-title"]');
  }
  getNoInquiriesText(): string {
    return 'You do not have inquiries yet.';
  }
  getInquiriesHeaders(): string[] {
    return ['Reference', 'Type', 'Date', 'Status', ''];
  }
  getStatusLabelPath(): string {
    return '[data-qa="component status"]';
  }
  private readonly selectors = {};
}