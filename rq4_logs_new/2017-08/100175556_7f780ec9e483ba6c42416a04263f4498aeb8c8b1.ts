
import {VotersService} from './voters.service'
import {ISession} from '../shared/event.model'
import {Observable} from 'rxjs/Rx'

describe('VoterService Test Class', ()=> {
    let voterService: VotersService,
    mockHttp 
    beforeEach(() =>{
        mockHttp= jasmine.createSpyObj('mockHttp', ['delete', 'post'])
        voterService = new VotersService(mockHttp)
    })
    
    describe('deleteVoter', ()=>{
        it('should be revmove the voter from voter list', ()=> {
            var session = {id:8, voters:["ashu", "raj"]}
            mockHttp.delete.and.returnValue(Observable.of(false))
            voterService.deleteVoter(3, <ISession>session, "raj");
            expect(session.voters.length).toBe(1);
            expect(session.voters[0]).toBe("ashu")
        })

        it('should call http.delete with the correct URL', ()=> {
            var session = {id:8, voters:["ashu", "raj"]}
            mockHttp.delete.and.returnValue(Observable.of(false))
            voterService.deleteVoter(3, <ISession>session, "raj");
            expect(mockHttp.delete).toHaveBeenCalledWith('/api/events/3/sessions/8/voters/raj')
        })
    })

    describe('addVoter', ()=>{
    
        // here we will test how to pass multiple parameter to http request as post method does
        it('should call http.post with the correct URL', ()=> {
            var session = {id:8, voters:["ashu"]}
            mockHttp.post.and.returnValue(Observable.of(false))
            voterService.addVoter(3, <ISession>session, "raj");
            expect(mockHttp.post).toHaveBeenCalledWith('/api/events/3/sessions/8/voters/raj', "{}", jasmine.any(Object))
        })
    })
})

