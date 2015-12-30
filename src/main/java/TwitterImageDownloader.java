import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by ahmetkucuk on 29/12/15.
 */
public class TwitterImageDownloader {

    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
    private Client hosebirdClient;
    private int numberOfImagesDownloaded = 0;

    public TwitterImageDownloader(List<String> trackTerms, Authentication hosebirdAuth, String imagePath) {
        configureStreamAPI(trackTerms, hosebirdAuth);
        configureDownloader(imagePath);

    }

    private void configureStreamAPI(List<String> trackTerms, Authentication hosebirdAuth) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(trackTerms);
        hosebirdEndpoint.addQueryParameter("include_entities", "true");
        hosebirdEndpoint.addQueryParameter("filter", "images");

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);

        hosebirdClient = builder.build();
    }

    private void configureDownloader(String path) {
        ImageDownloader.init(path, 10);
    }

    public void startDownloading() throws InterruptedException {
        downloadInAnotherThread();
    }

    private void downloadInAnotherThread() {
        hosebirdClient.connect();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (!hosebirdClient.isDone()) {
                    try {
                        useMessage(msgQueue.take());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                hosebirdClient.stop();
                ImageDownloader.getInstance().waitUntilDone();
            }
        }).start();
    }

    /**
     * Be careful - Download will not stop immediately
     */
    public void stopDownloading() {
        hosebirdClient.stop();
        ImageDownloader.getInstance().waitUntilDone();
    }

    public void useMessage(String msg) {

        JsonObject media1 = getMediaJsonObject(msg);

        if(media1 != null && media1.has("media_url")) {
            String url = media1.get("media_url").getAsString();
            if(url.contains("jpg") || url.contains("jpeg")) {
                numberOfImagesDownloaded = ImageDownloader.getInstance().addToDownloadQueue(url);
            }
        }
    }

    private JsonObject getMediaJsonObject(String msg) {
        JsonParser parser = new JsonParser();

        JsonObject jsonObject = parser.parse(msg).getAsJsonObject();
        if(jsonObject != null && jsonObject.has("entities") && jsonObject.get("entities").getAsJsonObject().has("media")) {

            JsonArray mediaJson = jsonObject.get("entities").getAsJsonObject().get("media").getAsJsonArray();
            if(mediaJson.size() > 0) {
                return mediaJson.get(0).getAsJsonObject();
            }
        }
        return null;
    }

    public int getNumberOfImagesDownloaded() {
        return numberOfImagesDownloaded;
    }

    public static void main(String[] args) throws InterruptedException {

        List<String> starWars = Lists.newArrayList("starwars", "star wars", "#starwars", "#theforceawakens", "the force awakens", "theforceawakens", "#starwars", "obi wan kenobi", "darth vader", "Luke Skywalker", "Yoda", "Princess Leia", "Han Solo", "The Emperor", "R2-D2", "Chewbacca", "Jubba the Hutt", "C-3PO");
        Authentication hosebirdAuth = new OAuth1("consumerKey", "consumerSecret", "token", "tokenSecret");
        String path = "/path/to/image/directory";
        TwitterImageDownloader twitterImageDownloader = new TwitterImageDownloader(starWars, hosebirdAuth, path);
        twitterImageDownloader.startDownloading();
        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        twitterImageDownloader.stopDownloading();
        System.out.println(twitterImageDownloader.getNumberOfImagesDownloaded());

    }

}
