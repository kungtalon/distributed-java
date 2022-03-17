package networking;

public interface OnRequestHandler {
    byte [] handleRequest(byte[] requestPayload);

    String getEndpoint();
}
