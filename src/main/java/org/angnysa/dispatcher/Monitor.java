package org.angnysa.dispatcher;

public interface Monitor<M> {
    void onMessageStarting(M message);
    void onMessageComplete(M message);
    void onMessageFailure(M message, Exception e);
}
