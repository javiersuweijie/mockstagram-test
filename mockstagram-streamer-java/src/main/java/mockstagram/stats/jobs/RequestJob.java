package mockstagram.stats.jobs;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;

import java.util.concurrent.Callable;

public class RequestJob<T> implements Callable {

    private String url;
    private Class returnClass;

    public RequestJob(String url, Class T) {
        this.url = url;
        this.returnClass = T;
    }

    @Override
    public T call() throws Exception {
        HttpResponse<T> response = Unirest.get(url).asObject(returnClass);
        return response.getBody();
    }
}
