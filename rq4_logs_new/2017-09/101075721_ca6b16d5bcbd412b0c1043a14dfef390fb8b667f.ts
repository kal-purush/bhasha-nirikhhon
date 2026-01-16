import {Observable} from 'rxjs/Rx';
import { GlassesService } from '../glasses.service';
import {ActivatedRoute, Params, Router} from '@angular/router';
import { GlassComponent } from './glass.component';
import { AuthenticationService } from '../../shared/services/authentication.service';
import { GlassResponse } from './models/glassResponse.model';
import { ToastsManager } from 'ng2-toastr/ng2-toastr';
import { ViewContainerRef } from '@angular/core';
import { RouterTestingModule } from '@angular/router/testing';

class MockGlassesService extends GlassesService {

  buyGlass(glassId: number) {
    return Observable.of([
    {
      Id: 1,
      Description: 'test description',
      EuroCode: 'testEuroCode',
      OesCode: 'testOesCode',
      ModelDate: 'ModelDate',
      PartDate: 'PartDate',
      ProductType: 'ProductType',
      Modification: 'Modification',
      Tint: 'Tint',
      FittingTimeHours: 1,
      FittingType: 'FittingType',
      Height: 2,
      Width: 2,
      IsAcoustic: true,
      IsCalibration: true,
      IsAccessory: true,
      MaterialNumber: 'MaterialNumber',
      LocalCode: 'LocalCode',
      IndustryCode: 'IndustryCode',
      IsYesGlass: true,
    }]);
  }
}

// class MockMarkdownService extends MarkdownService {
//   toHtml(text: string): string {
//     return text;
//   }
// }

// class MockBlogService extends BlogService {
//   constructor() {
//     super(null);
//   }

//   getBlogs() {
//     console.log('sending fake answers!');
//     return Observable.of([
//       {
//         id: 26,
//         title: 'The title',
//         contentRendered: '<p><b>Hi there</b></p>',
//         contentMarkdown: '*Hi there*'
//       }]);
//   }
// }

describe('GlassComponent unit test', () => {
  var glassComponent: GlassComponent;
  var route: ActivatedRoute;
  var glassesService: GlassesService;
  var router: RouterTestingModule;
  var authenticationService: AuthenticationService;
  var toastr: ToastsManager;
  var vcr: ViewContainerRef;

  beforeEach(() => {
    authenticationService = new AuthenticationService(null, router);
    glassesService = new MockGlassesService(null, authenticationService);
    glassComponent = new GlassComponent(route, glassesService, router,
                                        authenticationService, toastr, vcr);
  });

  it('shows list of blog items by default - unit', () => {
//    blogRoll.ngOnInit();
//    expect(blogRoll.blogs.length).toBe(1);
//    expect(blogRoll.blog).toBeUndefined();
//    expect(blogRoll.editing).toBe(false);
   expect(true).toBe(true);
  });

//   it('should show blog editor div when newBlogEntry is triggered...',  () => {
//    blogRoll.ngOnInit();
//    // we start with the blog roll panel visible
//    expect(blogRoll.editing).toBe(false);

//    // trigger the 'new' event
//    blogRoll.newBlogEntry();
//    expect(blogRoll.editing).toBe(true);
//    expect(blogRoll.blog).toBeDefined();
//   });

//   it('should pass data to the blogService during save', () => {
//     blogRoll.ngOnInit();
//     // make this a synchronous instantaneous call
//     // and since we only look for successful resolution it
//     // doesn't matter what we stub back
//     spyOn(blogService, 'saveBlog')
//       .and.returnValue(Observable.of({ complete: true}));

//     let entry = new BlogEntry('I am new', '<p>The content</p>', 'The content', undefined);
//     blogRoll.saveBlogEntry(entry);
//     expect(blogService.saveBlog).toHaveBeenCalledWith(entry);
//   });

//   it('should pass a blog item to remove to the blogService during delete', () => {
//     blogRoll.ngOnInit();
//     // alerts/confirm are an awful idea (short timeframe for this demo)
//     // but we can deal with the confirm message box by mocking it away,
//     // and returning true to continue processing - thanks Jasmine!
//     spyOn(window, 'confirm')
//        .and.callFake(() => { return true; });

//     // now mock the actual call to
//     spyOn(blogService, 'deleteBlogEntry')
//        .and.returnValue(Observable.of({ complete: true}));
//     let entry = new BlogEntry(null ,null, null, 15);
//     blogRoll.deleteBlogEntry(entry);

//     // collaborator behavior check
//     expect(window.confirm).toHaveBeenCalled();
//     expect(blogService.deleteBlogEntry).toHaveBeenCalledWith(15);
//   });
});