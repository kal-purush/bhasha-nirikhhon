import {JsonController, Get, Post, HttpCode, Param, Body } from 'routing-controllers'
import {Responses} from './entities'


@JsonController()
export default class ResponseController {

@Post('/responses')
@HttpCode(201)
createResponse(
    @Body() response:Response
) {
    return response.save()
}

@Get('/responses')
async allResponses(){
    const responses = await Response.find()
    return {responses}
}

@Get('/responses/id')
getResponse(
    @Param('id') id:number
){
    return response.findOne(id)
}

@Post('/anwsers')
@HttpCode(201)
createAnwser(
    @Body() answer:Answer
) {
    return answer.save()
}

@Get('/answers')
async allAnswers(){
    const answers = await Answer.find()
    return {answers}
}

@Get('/responses/id')
getAnswer(
    @Param('id') id:number
){
    return answer.findOne(id)
}

}
