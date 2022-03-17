package networking;

import com.squareup.okhttp.Callback;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class CallbackFuture extends CompletableFuture<Response> implements Callback {
    @Override
    public void onFailure(Request request, IOException e) {
        super.completeExceptionally(e);
    }

    @Override
    public void onResponse(Response response) throws IOException {
        super.complete(response);
    }
}
