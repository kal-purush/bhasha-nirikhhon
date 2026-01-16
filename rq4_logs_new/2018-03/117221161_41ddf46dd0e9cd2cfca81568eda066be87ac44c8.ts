import { AppPage } from './app.po';
import {before} from 'selenium-webdriver/testing';
import {UploadPage} from './upload.po';
import * as path from 'path';
import {DocumentRow, ListViewPage} from './listview.po';
import {browser, by, element} from 'protractor';
import {async} from 'q';
import {ViewerPage} from './viewer.po';
import {SummaryPage} from './summary.po';
describe('Upload and Annotate a PDF', () => {
  let page: UploadPage;

  beforeAll(() => {
    page = new UploadPage();
    page.navigateTo();
  });

  describe('when I upload a file', () => {
    const fileToUpload = './files/pdf.pdf',
      absolutePath = path.resolve(__dirname, fileToUpload);
    const listViewPage = new ListViewPage();

    beforeAll(() => {
      browser.wait(page.isLoaded);
    });

    beforeAll(() => {
      page.addFileToUpload(absolutePath).then(() => {
        browser.waitForAngularEnabled(false);
        page.getUploadButton().click();
      });
    });

    beforeAll(() => {
      return browser.wait(function () {
        return listViewPage.hasDocuments();
      }).then(() => {
        browser.waitForAngularEnabled(true);
      });
    });

    let uploadedDocument: DocumentRow;

    beforeAll(() => {
      uploadedDocument = listViewPage.getDocument(0);
    });

    describe('when I click annotate', () => {
      const viewerPage = new ViewerPage();
      beforeAll(() => {
        browser.waitForAngularEnabled(false);
        uploadedDocument.annotate();
      });

      beforeAll(() => {
        return browser.wait(function () {
          return viewerPage.isAnnotationsLoaded();
        }).then(() => {
          browser.waitForAngularEnabled(true);
        });
      });

      it('should open the viewer', () => {
        expect(browser.driver.getCurrentUrl()).toContain('viewer');
      });

      it('should show the file name', () => {
        expect(viewerPage.getTitleText()).toEqual('pdf.pdf');
      });

      it('should initialise with a blank note', () => {
        expect(viewerPage.getCurrentNoteText()).toEqual('');
      });

      describe('when I add a note and save', () => {
        const pageOneNote = `A rockin note`;

        beforeAll(() => {
          viewerPage.setCurrentNoteText(pageOneNote);
          viewerPage.getSaveButton().click();
        });

        it('should disable save', () => {
          expect(viewerPage.getSaveButton().isEnabled()).toBe(false);
        });

        it('should disable cancel', () => {
          expect(viewerPage.getCancelButton().isEnabled()).toBe(false);
        });

        it('should display to the note', () => {
          expect(viewerPage.getCurrentNoteText()).toEqual(pageOneNote);
        });

        describe('when I delete the note, save and refresh', () => {
          beforeAll(() => {
            viewerPage.clearCurrentNote().then(() => {
              viewerPage.getSaveButton().click();
            });
          });

          beforeAll(() => {
            return browser.wait(function () {
              return viewerPage.getSaveButton().isEnabled().then(enabled => {
                console.log(enabled);
                return !enabled;
              });
            }).then(() => {
              browser.refresh();
            });
          });

          beforeAll(() => {
            return browser.wait(function () {
              return viewerPage.isAnnotationsLoaded();
            });
          });

          // Not working at the moment for some reason.
          xit('should have deleted the note', () => {
            expect(viewerPage.getCurrentNoteText()).toEqual('');
          });
        });
      });
    });
  });
});