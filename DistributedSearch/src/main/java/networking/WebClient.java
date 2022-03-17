package networking;

import com.squareup.okhttp.*;

import java.util.concurrent.CompletableFuture;

public class WebClient {
    private OkHttpClient client;

    public WebClient() {
        this.client = new OkHttpClient();
    }

    public CompletableFuture<Response> sendAsyncTask(String url, byte[] payload) {
        RequestBody body = RequestBody.create(MediaType.parse("text/csv"), payload);

        Request request = new Request.Builder()
                .url(url)
                .header("Accept-Encoding", "identity")
                .post(body)
                .build();

        CallbackFuture future = new CallbackFuture();
        client.newCall(request).enqueue(future);
        return future;
    }

}