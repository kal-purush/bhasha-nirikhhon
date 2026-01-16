import request from 'supertest'
import app from '../app'

describe('Course Creation', () => {
    test('Request without token', async () => {
        const res = await request(app)
            .post('/course/create')
            .send({
                name: "Advanced Databases",
            })
            .set('Accept', 'application/json');

        expect(res.body).toEqual('A token is required for authentication');
    });

    test('Request with invalid token', async () => {
        const res = await request(app)
            .post('/course/create')
            .send({
                name: "Advanced Databases",
            })
            .set('Accept', 'application/json')
            .set('Cookie', ['jwt_token=abceyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJmNzk4Y2JmMS0zMTkyLTQ3OWQtYmZlZS05YjAzMjQ4MGJjYTkiLCJpYXQiOjE2NjE0OTE1NzEsImV4cCI6MTY2MTQ5NTE3MX0.lMiSiEnPcOXj40uUMoqoiOIbmaUkCShCtbxyOVuB2XQ; Max-Age=86400; Path=/; Expires=Sat, 27 Aug 2022 05:26:11 GMT; HttpOnly']);

        expect(res.body).toEqual('Invalid Token');
    });

    test('empty name', async () => {
        const res = await request(app)
            .post('/course/create')
            .send({
                name: "",
            })
            .set('Accept', 'application/json')
            .set('Cookie', ['jwt_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJmNzk4Y2JmMS0zMTkyLTQ3OWQtYmZlZS05YjAzMjQ4MGJjYTkiLCJpYXQiOjE2NjE0OTE1NzEsImV4cCI6MTY2MTQ5NTE3MX0.lMiSiEnPcOXj40uUMoqoiOIbmaUkCShCtbxyOVuB2XQ; Max-Age=86400; Path=/; Expires=Sat, 27 Aug 2022 05:26:11 GMT; HttpOnly']);

        expect(res.body).toEqual('Course Name Cannot be Empty!');
    });

    test('vlid name and token', async () => {
        const res = await request(app)
            .post('/course/create')
            .send({
                name: "Advanced Databases",
            })
            .set('Accept', 'application/json')
            .set('Cookie', ['jwt_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJmNzk4Y2JmMS0zMTkyLTQ3OWQtYmZlZS05YjAzMjQ4MGJjYTkiLCJpYXQiOjE2NjE0OTE1NzEsImV4cCI6MTY2MTQ5NTE3MX0.lMiSiEnPcOXj40uUMoqoiOIbmaUkCShCtbxyOVuB2XQ; Max-Age=86400; Path=/; Expires=Sat, 27 Aug 2022 05:26:11 GMT; HttpOnly']);

        expect(res.body).toHaveProperty('courseName');
    });

});