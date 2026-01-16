import {cloneDeep} from "lodash";
import {removeMetadata, viewFilter} from "./metadata.util";

const subject = {
  a: {
    aa:'aa',
    $view: ['RoleA', 'RoleB', 'RoleC'],
    inner:{
      aaa:'aaa',
      $view: ['RoleD'],
    }
  },
  b: 'b',
  c: {
    d: 'd',
    e: 'e',
    $view: ['RoleA', 'RoleB', 'RoleC']
  },
  f: 'f',
  h: {
    i: 'i',
    j: 'j',
    $view: ['RoleD'],
    inner:{
      z:'z',
      $view: ['RoleA', 'RoleB', 'RoleC']
    },
    $other: 'other'
  },
  k: [
    'l',
    'm',
    {
      o: 'o',
      p: 'p',
      $view: ['RoleA', 'RoleB', 'RoleC']
    },
    'n',
  ]
}

describe('metadata.util', () => {
  describe('viewFilter()', () => {
    it('should filter RoleD correctly', () => {
      expect(viewFilter(cloneDeep(subject), ['RoleD'])).toEqual({
        "b": "b",
        "f": "f",
        "h": {
          "$other": "other",
          "$view": [
            "RoleD"
          ],
          "i": "i",
          "j": "j"
        },
        "k": [
          "l",
          "m",
          "n"
        ]
      })
    })
    it('should filter RoleA correctly', () => {
      expect(viewFilter(cloneDeep(subject), ['RoleA'])).toEqual({
        "a": {
          "$view": [
            "RoleA",
            "RoleB",
            "RoleC"
          ],
          "aa": "aa"
        },
        "b": "b",
        "c": {
          "$view": [
            "RoleA",
            "RoleB",
            "RoleC"
          ],
          "d": "d",
          "e": "e"
        },
        "f": "f",
        "k": [
          "l",
          "m",
          {
            "$view": [
              "RoleA",
              "RoleB",
              "RoleC"
            ],
            "o": "o",
            "p": "p"
          },
          "n"
        ]
      })
    })
    it('should filter no role correctly', () => {
      expect(viewFilter(cloneDeep(subject), [])).toEqual({
        "b": "b",
        "f": "f",
        "k": [
          "l",
          "m",
          "n"
        ]
      })
    })
  })
  describe('removeMetadata()', () => {
    it('should remove all metadata', () => {
      expect(removeMetadata(cloneDeep(subject))).toEqual({
        "a": {
          "aa": "aa",
          "inner": {
            "aaa": "aaa"
          }
        },
        "b": "b",
        "c": {
          "d": "d",
          "e": "e"
        },
        "f": "f",
        "h": {
          "i": "i",
          "inner": {
            "z": "z"
          },
          "j": "j"
        },
        "k": [
          "l",
          "m",
          {
            "o": "o",
            "p": "p"
          },
          "n"
        ]
      })
    })
  })
})