// args.apiDoc needs to be a js object.  This file could be a json file, but we can't add
// comments in json files.
module.exports = {
  swagger: '2.0',

  // all routes will now have /api/v1 prefixed.
  basePath: '/origoserver/api-test/v1',

  schemes: ['https'],
  
  info: {
    title: 'TEST API GIS',
    description: 'Test API from the GIS department',
    version: '1.0',
  },

  definitions: {
    EstateSearchResponse: {
      type: 'object',
      properties: {
        address: {
          description: 'The adress of the estate.',
          type: 'string'
        },
        designation: {
          description: 'The designation of the estate.',
          type: 'string'
        },
        objectidentifier: {
          description: 'The unique identifier of the estate.',
          type: 'string'
        }
      }
    },
    EstateData: {
      type: 'object',
      properties: {
        designation: {
          description: 'The designation of the estate.',
          type: 'string'
        },
        objectidentifier: {
          description: 'The unique identifier of the estate.',
          type: 'string'
        },
        totalArea: {
          description: 'The total area of the estate.',
          type: 'integer'
        },
        totalAreaWater: {
          description: 'The total area of the estate which are over water.',
          type: 'integer'
        },
        totalAreaLand: {
          description: 'The total area of the estate which are over land.',
          type: 'integer'
        },
        ownership: {
          description: 'The current ownership of the estate.',
          type: 'array',
          items: {
            "$ref": "#/definitions/TitleDeed"
          }
        },
        mortage: {
          description: 'The mortages of the estate.',
          type: 'array',
          items: {
            "$ref": "#/definitions/Mortage"
          }
        },
        previousOwnership: {
          description: 'The previous ownerships of the estate.',
          type: 'array',
          items: {
            "$ref": "#/definitions/TitleDeed"
          }
        },
        ownerChanges: {
          description: 'The changes of owning of estate.',
          type: 'array',
          items: {
            "$ref": "#/definitions/OwnerChanges"
          }
        },
        actions: {
          description: 'The actions performed on the estate.',
          type: 'array',
          items: {
            "$ref": "#/definitions/Actions"
          }
        }
      }
    },
    TitleDeed: {
      type: 'object',
      properties: {
        type: {
          description: 'Type of ownership.',
          type: 'string'
        },
        objectidentifier: {
          description: 'The unique identifier of the ownership.',
          type: 'string'
        },
        enrollmentDay: {
          description: 'The date of enrollment.',
          type: 'string'
        },
        decision: {
          description: 'The decision of the ownership.',
          type: 'string'
        },
        share: {
          description: 'The share of ownership.',
          type: 'string'
        },
        diaryNumber: {
          description: 'References to dairy.',
          type: 'array',
          items: {
            type: "string"
          }
        },
        versionValidFrom: {
          description: 'From when the ownership is valid.',
          type: 'string'
        },
        owner: {
          description: 'The owners of a estate.',
          type: 'object',
          properties: {
            idnumber: {
              description: 'The id number of owner i.e. organisation number or person number.',
              type: 'string'
            },
            name: {
              description: 'The name of owner person or organisation.',
              type: 'string'
            },
            coAddress: {
              description: 'The c/o address of owner person or organisation.',
              type: 'string'
            },
            address: {
              description: 'The address of owner person or organisation.',
              type: 'string'
            },
            postalCode: {
              description: 'The postal code of owner person or organisation.',
              type: 'string'
            },
            city: {
              description: 'The postal city of owner person or organisation.',
              type: 'string'
            }
          }
        }
      }
    },
    Mortage: {
      type: 'object',
      properties: {
        objectidentifier: {
          description: 'The unique identifier of the mortage.',
          type: 'string'
        },
        type: {
          description: 'The type of burdens.',
          type: 'string'
        },
        priorityOrder: {
          description: 'The order of burdens on the estate.',
          type: 'integer'
        },
        enrollmentDay: {
          description: 'The date of enrollment.',
          type: 'string'
        },
        decision: {
          description: 'The decision of the mortage.',
          type: 'string'
        },
        diaryNumber: {
          description: 'References to dairy.',
          type: 'array',
          items: {
            type: "string"
          }
        },
        mortageType: {
          description: 'The type of the mortage.',
          type: 'string'
        },
        mortageAmount: {
          description: 'The amount of the mortage.',
          type: 'object',
          properties: {
            currency: {
              description: 'The currency the mortage amount.',
              type: 'string'
            },
            sum: {
              description: 'The mortage amount.',
              type: 'integer'
            }
          }
        }
      }
    },
    OwnerChanges: {
      type: 'object',
      properties: {
        objectidentifier: {
          description: 'The unique identifier of the ownership.',
          type: 'string'
        },
        acquisition: {
          description: 'The acquisition.',
          type: 'array',
          items: {
            "$ref": "#/definitions/Acquisition"
          }
        },
        purchasePrice: {
          description: 'The price of the purchase.',
          type: 'object',
          properties: {
            objectidentifier: {
              description: 'The unique identifier of the purchase price.',
              type: 'string'
            },
            purchasePriceImmovableProperty: {
              description: 'The price of immovable property of the estate.',
              type: 'object',
              properties: {
                currency: {
                  description: 'The currency the purchase sum.',
                  type: 'string'
                },
                sum: {
                  description: 'The purchase sum.',
                  type: 'integer'
                }
              }
            },
            purchasePriceType: {
              description: 'The type of the purchase.',
              type: 'string'
            }
          }
        },
        transfer: {
          description: 'The transfer of a acquisition.',
          type: 'array',
          items: {
            "$ref": "#/definitions/Transfer"
          }
        }
      }
    },
    Acquisition: {
      type: 'object',
      properties: {
        objectidentifier: {
          description: 'The unique identifier of the acquisition.',
          type: 'string'
        },
        enrollmentDay: {
          description: 'The enrollment day of the acquisition.',
          type: 'string'
        },
        decision: {
          description: 'The decision of the acquisition.',
          type: 'string'
        },
        share: {
          description: 'The share of the acquisition.',
          type: 'string'
        },
        acquisitionDay: {
          description: 'The day of the acquisition.',
          type: 'string'
        },
        acquisitionType: {
          description: 'The type of the acquisition.',
          type: 'string'
        },
        registeredOwnership: {
          description: 'The unique identifier of the registered ownership.',
          type: 'string'
        }
      }
    },
    Transfer: {
      type: 'object',
      properties: {
        objectidentifier: {
          description: 'The unique identifier of the acquisition.',
          type: 'string'
        },
        share: {
          description: 'The share of the acquisition.',
          type: 'string'
        },
        registeredOwnership: {
          description: 'The unique identifier of the registered ownership.',
          type: 'string'
        }
      }
    },
    Actions: {
      type: 'object',
      properties: {
        actionType1: {
          description: 'The first type of an action.',
          type: 'string'
        },
        actionType2: {
          description: 'The second type of an action.',
          type: 'string'
        },
        fileDesignation: {
          description: 'The designation of the file of the action.',
          type: 'string'
        },
        actionDate: {
          description: 'The date of when the action is performed.',
          type: 'string'
        },
        objectidentifier: {
          description: 'The unique identifier of the action.',
          type: 'string'
        },
        littera: {
          description: 'Reference to estate in the action.',
          type: 'string'
        },
        runningNumber: {
          description: 'A number showing in which order the actions is performed.',
          type: 'integer'
        }
      }
    }
  },

  // paths are derived from args.routes.  These are filled in by fs-routes.
  paths: {},
};