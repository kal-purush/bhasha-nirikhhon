import Day from "../src/Activities/Day";
import FreeDayItineary from "../src/Activities/FreeDayItineary";
import Hotel from "../src/Hotel/Hotel";
import Person from "../src/Hotel/Person";
import Room from "../src/Hotel/Room";
import Trip from "../src/Trip/Trip";
import TripSegment from "../src/Trip/TripSegment";

const beforeDate = new Date(Date.parse("2018-01-01"));
const afterDate = new Date(Date.parse("2020-01-01"));
const startdate = new Date(Date.parse("2019-01-01"));
const date2 = new Date(Date.parse("2019-01-02"));
const date3 = new Date(Date.parse("2019-01-03"));
const transitDay1 = new Date(Date.parse("2019-01-04"));
const transitDay2 = new Date(Date.parse("2019-01-05"));
const endDate = new Date(Date.parse("2019-01-06"));


const it1 = new FreeDayItineary("1","");
const it2 = new FreeDayItineary("2","");
const it3 = new FreeDayItineary("3","");
const it4 = new FreeDayItineary("4","");
const it5 = new FreeDayItineary("5","");
const it6 = new FreeDayItineary("6","");

const day1 = new Day("1", "?", startdate, [it1]);
const day2 = new Day("2", "?", date2, [it2]);
const day3 = new Day("3", "?", date3, [it3]);
const day4 = new Day("4", "?", transitDay1, [it4]);
const day5 = new Day("5", "?", transitDay2, [it5]);
const day6 = new Day("6", "?", endDate, [it6]);

const person1: Person = {id:"1",name:"2"}

const rm1 = new Room("1", "", startdate, transitDay1, 12, 1, [person1]);
const rm2 = new Room("2", "", transitDay1, transitDay2, 12, 1, [person1]);
const rm3 = new Room("3", "", transitDay2, endDate, 12, 1, [person1]);

const overlappingRoom = new Room("4", "", date2, transitDay2, 12, 1, [person1]);

const ht1 = new Hotel("1", "", startdate, transitDay1, "", [rm1]);
const ht2 = new Hotel("2", "", transitDay1, transitDay2, "", [rm2]);
const ht3 = new Hotel("3", "", transitDay2, endDate, "", [rm3]);

const overlappingHotel = new Hotel("4", "", date2, transitDay2,"",[overlappingRoom]);

const split1 = new TripSegment("1", "", startdate, transitDay1, [person1], [ht1], [day1, day2, day3, day4]);
const split2 = new TripSegment("2", "", transitDay1, transitDay2, [person1], [ht2], [day4, day5]);
const split3 = new TripSegment("3", "", transitDay2, endDate, [person1], [ht3], [day5, day6]);

const overlappingSplit = new TripSegment("4", "", date2, transitDay2, [person1], [overlappingHotel], [day2, day3, day4, day5]);


const invalidBefore = new Day("1", "2", beforeDate);
const invalidAfter = new Day("1", "2", afterDate);

describe("entire trip", () => {
  test("valid Test data", () => {
    expect(split1.wellformed()).toBeTruthy()
    expect(split2.wellformed()).toBeTruthy()
    expect(split3.wellformed()).toBeTruthy()
    expect(overlappingSplit.wellformed()).toBeTruthy()

  })
  test("split integration", () => {
    const testTrip = new Trip("1", "", startdate, endDate, [person1]);
    testTrip.addSplit(split1);
    testTrip.addSplit(split2);
    testTrip.addSplit(split3);
    expect(testTrip.getSplit(split1.id)).toStrictEqual(split1);
    expect(testTrip.getSplit(split2.id)).toStrictEqual(split2);
    expect(testTrip.getSplit(split3.id)).toStrictEqual(split3);
    expect(testTrip.listDays()).toStrictEqual([day1,day2,day3,day4,day5,day6]);
    
    expect(() => testTrip.addSplit(split1)).toThrow();
    expect(() => testTrip.addSplit(overlappingSplit)).toThrow();
    expect(testTrip.wellformed()).toBeTruthy();
    expect(() => testTrip.removeSplit(overlappingSplit.id)).toThrow();
    expect(testTrip.removeSplit(split1.id)).toStrictEqual(split1);
    expect(testTrip.wellformed()).toBeFalsy();
    expect(testTrip.removeSplit(split2.id)).toStrictEqual(split2);
    expect(testTrip.removeSplit(split3.id)).toStrictEqual(split3);
    expect(testTrip.splits.length).toBe(0);
  });
})