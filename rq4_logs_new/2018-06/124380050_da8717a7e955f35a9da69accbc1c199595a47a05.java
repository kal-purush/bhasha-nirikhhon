package org.duckdns.gong.ask.handlers;

import com.amazon.ask.dispatcher.request.handler.HandlerInput;
import com.amazon.ask.dispatcher.request.handler.RequestHandler;
import com.amazon.ask.model.DialogState;
import com.amazon.ask.model.Intent;
import com.amazon.ask.model.IntentRequest;
import com.amazon.ask.model.Request;
import com.amazon.ask.model.Response;
import org.duckdns.gong.ask.Client;
import java.util.Optional;
import static com.amazon.ask.request.Predicates.intentName;

public class NumberMessageIntentHandler implements RequestHandler {
    private Client cl;

    @Override
    public boolean canHandle(HandlerInput input) {
        return input.matches(intentName("NumberMessageIntent"));
    }

    @Override
    public Optional<Response> handle(HandlerInput input) {
        cl = new Client();
        Request request = input.getRequestEnvelope().getRequest();
        IntentRequest intentRequest = (IntentRequest) request;
        Intent intent = intentRequest.getIntent();
        String speechText, number, content;
        DialogState dialogueState = intentRequest.getDialogState();

        // 알렉사와 대화과정을 거치기 위하여
        if (dialogueState.equals(DialogState.STARTED)) {
            return input.getResponseBuilder()
                    .addDelegateDirective(intent)
                    .build();
        } else if (dialogueState.equals(DialogState.IN_PROGRESS)) {
            return input.getResponseBuilder()
                    .addDelegateDirective(intent)
                    .build();
        } else {
            // 슬롯 값들을 가져옴
            number = intent.getSlots().get("Number").getValue();
            content = intent.getSlots().get("Content").getValue();

            cl.enterServer();
            speechText = "Ok. I will send message request.";
            // 서버로 메세지를 보낼번호와 내용을 추가하여 요청을 전송
            cl.sendStr("req message " + number + " " + content);
            cl.closeConnect();

            return input.getResponseBuilder()
                    .withSpeech(speechText)
                    .withSimpleCard("NumberMessageIntent", speechText)
                    .build();
        }
    }
}