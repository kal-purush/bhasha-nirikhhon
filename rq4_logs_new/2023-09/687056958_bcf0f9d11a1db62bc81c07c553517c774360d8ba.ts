import { Request, Response } from 'express';
import chai, { expect } from 'chai'
import sinon from 'sinon'
import sinonChai from 'sinon-chai' 
import * as storeService from '../../src/services/storesService';
import storeController from '../../src/controllers/storeController'
import { allStores, newStore } from '../mocks/storesMocks';

chai.use(sinonChai)

describe('Stores Controller', () => {

describe('list stores', () => {
  afterEach(() => {
    sinon.reset()
  })

  it('List all stores', async() => {

    sinon.stub(storeService, 'list').resolves(allStores)
  
    const req = {} as Request
    const res = {} as Response
  
    res.status = sinon.stub().returns(res)
    res.json = sinon.stub().returns(res)

    await storeController.list(req, res)
    expect(res.status).to.have.been.calledWith(200)
    expect(res.json).to.have.been.calledWith(allStores)
  })

  })


describe('Create user', () => {
  afterEach(() => {
    sinon.reset()
  })

  it('Successfully create new store', async() => {
    sinon.stub(storeService, 'create').resolves(newStore)
  
    const req = {} as Request
    const res = {} as Response
  
    res.status = sinon.stub().returns(res)
    res.json = sinon.stub().returns(res)

    req.body = {
      "name": "Nova loja do bairro",
      "urlLogo": "https://www.prisma.io/nextjs",
      "address": "Rua das Araras, 450 Bairro Planices",
      "ownerId": 2
    }
      
    await storeController.create(req, res);

    expect(res.status).to.have.been.calledWith(201);
    expect(res.json).to.have.been.calledWith(newStore)
  })

})

// describe('Edit user', () => {
//   afterEach(() => {
//     sinon.reset()
//   })

//   it('Successfully edit user name', async() => {
//     sinon.stub(storeService, 'editName').resolves(newUser)
  
//     const req = {} as Request
//     const res = {} as Response

//     req.params = { 
//       id: "1"
//     }
  
//     req.body = {
//       "name": "Maria das Graças",
//     }
//     res.status = sinon.stub().returns(res)
//     res.json = sinon.stub().returns(res)

      
//     await storeController.editName(req, res);

//     expect(res.status).to.have.been.calledWith(200);
//     expect(res.json).to.have.been.calledWith(newUser)
    
//   })

// })

describe('Remove store', () => {
  afterEach(() => {
    sinon.reset()
  })
  it('Successfully remove store', async() => {
    sinon.stub(storeService, 'remove').resolves()
  
    const req = {} as Request
    const res = {} as Response

    req.params = { 
      id: "1"
    }
  
    res.status = sinon.stub().returns(res)
    res.json = sinon.stub().returns(res)

      
    await storeController.remove(req, res);

    expect(res.status).to.have.been.calledWith(204);
  })

})
})