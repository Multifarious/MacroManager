package com.fasterxml.mama;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NodeInfo {
    public final String state;
    public final long connectionID;

    @JsonCreator
    public NodeInfo(@JsonProperty("state") String state,
            @JsonProperty("connectionID") long connectionID) {
        this.state = state;
        this.connectionID = connectionID;
    }

    @Override
    public String toString() {
        return "[NodeInfo: state="+state+", connectionID="+connectionID+"]";
    }
}
