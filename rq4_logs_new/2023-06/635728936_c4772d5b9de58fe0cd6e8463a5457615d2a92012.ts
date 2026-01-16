/**
 * @openapi
 * components:
 *   schemas:
 *     CreateUserInput:
 *       type: object
 *       required:
 *         - name
 *         - surname
 *         - password
 *         - birthday
 *         - email
 *       properties:
 *         name:
 *           type: string
 *           default: Poghos
 *         surname:
 *           type: string
 *           default: Poghosyan
 *         password:
 *           type: string
 *           default: strongPassword
 *         birthday:
 *           type: string
 *           default: 2001-01-07T00:00:00.000Z
 *         email:
 *           type: string
 *           default: poghospoghsyan@gmail.com
 * 
 *     LoginUserInput:
 *       type: object
 *       properties:
 *         email:
 *           type: string
 *           default: poghospoghsyan@gmail.com
 *         password:
 *           type: string      
 *           default: strongPassword
 * 
 *     LoginUserResponse:
 *       type: object
 *       properties:
 *         user:
 *           $ref: '#/components/schemas/User'
 *         token:
 *           type: string
 * 
 *     User:
 *       type: object
 *       properties:
 *         id:
 *           type: integer
 *         name:
 *           type: string
 *         surname:
 *           type: string
 *         birthday:
 *           type: string
 *         email:
 *           type: string
*/