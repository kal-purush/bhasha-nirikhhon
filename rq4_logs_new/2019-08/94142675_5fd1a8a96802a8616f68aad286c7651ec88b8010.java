package net.vivialconnect.model.connector;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConnectorPaginatedPhoneNumbers {

    @JsonProperty("connector")
    private Connector connector;

    @JsonProperty("count")
    private int count;

    @JsonProperty("next")
    private int next;

    @JsonProperty("pages")
    private int pages;

    @JsonProperty("previous")
    private int previous;

    public Connector getConnector() {
        return setupConnector(connector);
    }

    private Connector setupConnector(Connector connector){

        if(connector != null){

            connector.setPages(pages);
            connector.setPreviousPage(previous);
            connector.setNextPage(next);
            connector.setPhoneNumbersCount(count);

        }

        return  connector;
    }

}