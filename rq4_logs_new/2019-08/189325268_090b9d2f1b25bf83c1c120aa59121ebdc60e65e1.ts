// import { async, ComponentFixture, TestBed } from '@angular/core/testing';
// import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
// import { By } from '@angular/platform-browser';
// import { CommonModule } from '@angular/common';
// import { HttpErrorResponse } from '@angular/common/http';

// import { MatGridListModule, MatSelectModule, MatCardModule, MatRadioModule } from '@angular/material';
// import { Subject } from 'rxjs';
// import { ToastrService } from 'ngx-toastr';

// import { ConvertNaNPipe } from 'src/app/pipes/convert-nan.pipe';
// import { GroupService } from 'src/app/groups/group.service';
// import { UserService } from '../user.service';
// import { UserStatsComponent } from './user-leaderboard.component';
// import { User } from '../user.model';

// describe('UserLeaderboardComponent', () => {
//   let component: UserStatsComponent;
//   let fixture: ComponentFixture<UserStatsComponent>;
//   let userService: SpyObject<UserService>;
//   let groupService: SpyObject<GroupService>;
//   let toastr: SpyObject<ToastrService>;
//   const allTimeStats$ = new Subject<User[]>();
//   const inGroupStats$ = new Subject<User[]>();
//   const userOne: User = {
//     username: 'fake',
//     stats: {
//       catches: 0,
//       drops: 1,
//       sinkers: 2,
//       points: 3,
//       fifas: 4,
//       gamesWon: 5,
//       gamesLost: 6
//     }
//   };

//   const userTwo: User = {
//     username: 'fake-2',
//     stats: {
//       catches: 1,
//       drops: 2,
//       sinkers: 3,
//       points: 4,
//       fifas: 5,
//       gamesWon: 6,
//       gamesLost: 7
//     }
//   };

//   beforeEach(async(() => {
//     userService = createSpyObject(['getUserStatsInGroup', 'getUserStatsAllTime']);
//     userService.getUserStatsAllTime.mockReturnValue(allTimeStats$.asObservable());
//     userService.getUserStatsInGroup.mockReturnValue(inGroupStats$.asObservable());

//     groupService = createSpyObject(['getCurrentGroup']);
//     toastr = createSpyObject(['error']);

//     TestBed.configureTestingModule({
//       imports: [
//         CommonModule,
//         BrowserAnimationsModule,
//         MatGridListModule,
//         MatSelectModule,
//         MatCardModule,
//         MatRadioModule
//       ],
//       declarations: [
//         ConvertNaNPipe,
//         UserStatsComponent
//       ],
//       providers: [
//         { provide: UserService, useValue: userService },
//         { provide: GroupService, useValue: groupService },
//         { provide: ToastrService, useValue: toastr }
//       ]
//     })
//     .compileComponents();
//   }));

//   beforeEach(() => {
//     fixture = TestBed.createComponent(UserStatsComponent);
//     component = fixture.componentInstance;
//     fixture.detectChanges();
//   });

//   it('should create', () => {
//     expect(component).toBeTruthy();
//   });

//   describe('ngOnInit behavior', () => {
//     test('gets correct stats when in group', () => {
//       jest.spyOn(userService, 'getUserStatsInGroup');
//       groupService.getCurrentGroup.mockReturnValue('groupName');
//       component.ngOnInit();
//       expect(component.isInGroup).toBeTruthy();
//       expect(userService.getUserStatsInGroup).toHaveBeenCalled();

//       const users = [userOne, userTwo];
//       inGroupStats$.next(users);
//       expect(component.inGroupUsersStats).toEqual(users);
//       expect(component.users).toEqual(users);
//     });

//     test('gets correct stats when not in group', () => {
//       jest.spyOn(userService, 'getUserStatsAllTime');
//       expect(component.isInGroup).toBeFalsy();
//       expect(userService.getUserStatsAllTime).toHaveBeenCalled();

//       const users = [userOne, userTwo];
//       allTimeStats$.next(users);
//       expect(component.allTimeUsersStats).toEqual(users);
//       expect(component.users).toEqual(users);
//     });

//     test('handles http error response 401', () => {
//       jest.spyOn(toastr, 'error');
//       allTimeStats$.error(new HttpErrorResponse({ status: 401 }));
//       expect(toastr.error).toHaveBeenCalled();
//     });

//     test('handles http error response 500', () => {
//       jest.spyOn(toastr, 'error');
//       allTimeStats$.error(new HttpErrorResponse({ status: 500 }));
//       expect(toastr.error).toHaveBeenCalled();
//     });
//   });

//   describe('in group and all time stat filtering', () => {
//     test('set all time works', () => {
//       jest.spyOn(component, 'setAllTime');
//       component.isInGroup = true;
//       component.allTimeUsersStats = [userOne];
//       fixture.detectChanges();
//       const allTimeRadioButton = fixture.debugElement.query(By.css('[value="all-time"]')).nativeElement;
//       allTimeRadioButton.click();
//       expect(component.setAllTime).toHaveBeenCalled();
//       expect(component.users).toEqual([userOne]);
//     });

//     test('set with group works', () => {
//       jest.spyOn(component, 'setWithinGroup');
//       component.isInGroup = true;
//       component.inGroupUsersStats = [userTwo];
//       fixture.detectChanges();
//       const withinGroupRadioButton = fixture.debugElement.query(By.css('[value="within-group"]')).nativeElement;
//       withinGroupRadioButton.click();
//       expect(component.setWithinGroup).toHaveBeenCalled();
//       expect(component.users).toEqual([userTwo]);
//     });
//   });

//   describe('ascending and descending stat filters', () => {
//     test('set ascending works', () => {
//       jest.spyOn(component, 'setAscending');
//       jest.spyOn(component, 'onSelection');
//       component.isInGroup = true;
//       fixture.detectChanges();
//       const setAscendingRadioButton = fixture.debugElement.query(By.css('[value="ascending"]')).nativeElement;
//       setAscendingRadioButton.click();
//       expect(component.setAscending).toHaveBeenCalled();
//       expect(component.onSelection).toHaveBeenCalledWith({ value: component.currentSortMethod });
//     });

//     test('set descending works', () => {
//       jest.spyOn(component, 'setDescending');
//       jest.spyOn(component, 'onSelection');
//       component.isInGroup = true;
//       fixture.detectChanges();
//       const setDescendingRadioButton = fixture.debugElement.query(By.css('[value="descending"]')).nativeElement;
//       setDescendingRadioButton.click();
//       expect(component.setDescending).toHaveBeenCalled();
//       expect(component.onSelection).toHaveBeenCalledWith({ value: component.currentSortMethod });
//     });
//   });

//   describe('onSelection behavior', () => {
//     const users = [userOne, userTwo];
//     beforeEach(() => {
//       component.users = users;
//     });

//     describe('when descending', () => {
//       beforeEach(() => {
//         component.ascending = false;
//       });

//       test('sorts by number of wins works ', () => {
//         component.onSelection({ value: 'wins' });
//         expect(component.users[0].stats.gamesWon > component.users[1].stats.gamesWon).toBeTruthy();
//       });

//       test('sorts by number of losses works', () => {
//         component.onSelection({ value: 'losses' });
//         expect(component.users[0].stats.gamesLost > component.users[1].stats.gamesLost).toBeTruthy();
//       });

//       test('sorts by number of losses works', () => {
//         component.onSelection({ value: 'W/L' });
//         const winLossOne = component.users[0].stats.gamesWon / (component.users[0].stats.gamesWon + component.users[0].stats.gamesLost);
//         const winLossTwo = component.users[1].stats.gamesWon / (component.users[1].stats.gamesWon + component.users[1].stats.gamesLost);
//         expect(winLossOne > winLossTwo).toBeTruthy();
//       });

//       test('sorts by number of catches works', () => {
//         component.onSelection({ value: 'catches' });
//         expect(component.users[0].stats.catches > component.users[1].stats.catches).toBeTruthy();
//       });

//       test('sorts by number of points works', () => {
//         component.onSelection({ value: 'points' });
//         expect(component.users[0].stats.points > component.users[1].stats.points).toBeTruthy();
//       });

//       test('sorts by number of fifas works ', () => {
//         component.onSelection({ value: 'fifas' });
//         expect(component.users[0].stats.fifas > component.users[1].stats.fifas).toBeTruthy();
//       });

//       test('sorts by number of drops works', () => {
//         component.onSelection({ value: 'points' });
//         expect(component.users[0].stats.drops > component.users[1].stats.drops).toBeTruthy();
//       });

//       test('sorts by number of sinkers works', () => {
//         component.onSelection({ value: 'sinkers' });
//         expect(component.users[0].stats.sinkers > component.users[1].stats.sinkers).toBeTruthy();
//       });
//     });

//     describe('when ascending', () => {
//       beforeEach(() => {
//         component.ascending = true;
//       });

//       test('sorts by number of wins works', () => {
//         component.onSelection({ value: 'wins' });
//         expect(component.users[0].stats.gamesWon < component.users[1].stats.gamesWon).toBeTruthy();
//       });

//       test('sorts by number of losses works', () => {
//         component.onSelection({ value: 'wins' });
//         expect(component.users[0].stats.gamesWon < component.users[1].stats.gamesWon).toBeTruthy();
//       });

//       test('sorts by number of losses works', () => {
//         component.onSelection({ value: 'W/L' });
//         const winLossOne = component.users[0].stats.gamesWon / (component.users[0].stats.gamesWon + component.users[0].stats.gamesLost);
//         const winLossTwo = component.users[1].stats.gamesWon / (component.users[1].stats.gamesWon + component.users[1].stats.gamesLost);
//         expect(winLossOne < winLossTwo).toBeTruthy();
//       });

//       test('sorts by number of catches works', () => {
//         component.onSelection({ value: 'catches' });
//         expect(component.users[0].stats.gamesWon < component.users[1].stats.gamesWon).toBeTruthy();
//       });

//       test('sorts by number of points works', () => {
//         component.onSelection({ value: 'points' });
//         expect(component.users[0].stats.points < component.users[1].stats.points).toBeTruthy();
//       });

//       test('sorts by number of drops works', () => {
//         component.onSelection({ value: 'drops' });
//         expect(component.users[0].stats.drops < component.users[1].stats.drops).toBeTruthy();
//       });

//       test('sorts by number of fifas works', () => {
//         component.onSelection({ value: 'fifas' });
//         expect(component.users[0].stats.fifas < component.users[1].stats.fifas).toBeTruthy();
//       });

//       test('sorts by number of sinkers works', () => {
//         component.onSelection({ value: 'sinkers' });
//         expect(component.users[0].stats.sinkers < component.users[1].stats.sinkers).toBeTruthy();
//       });
//     });
//   });
// });