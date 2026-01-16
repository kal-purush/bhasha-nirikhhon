export default {
  "openapi": "3.0.1",
  "info": {
    "title": "Budget api",
    "description": "Documentation of a budget API",
    "contact": {
      "url": "https://github.com/DouglasD18",
      "email": "daguiaralcantara@gmail.com"
    },
    "version": "1.0.0"
  },
  "basePath": "/api",
  "paths": {
    "/budget/": {
      "post": {
        "description": "Calculate budget",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "userId": {
                  "type": "number"
                },
                "productsId": {
                  "type": "array",
                  "items": {
                    "type": "integer"
                  }
                }
              },
              "examples": {
                "userId": 1,
                "productsId": [1, 2]
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "Created",
            "content": {
              "application/json": {
                "body": {
                  "type": "number",
                  "example": 7410.2
                }
              }
            }
          },
          "400": {
            "description": "Bad Request"
          },
          "404": {
            "description": "Not Found"
          },
          "500": {
            "description": "Internal Error"
          }
        }
      }
    }
  }
}