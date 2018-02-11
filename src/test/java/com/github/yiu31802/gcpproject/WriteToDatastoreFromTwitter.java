package com.github.yiu31802.gcpproject;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Entity.Builder;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.type.LatLng;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import twitter4j.GeoLocation;
import twitter4j.MediaEntity;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class WriteToDatastoreFromTwitter {
    private static Value sValue(String k) {
        return makeValue(k).setExcludeFromIndexes(true).build();
    }

    private static Value bValue(Boolean k) {
        return makeValue(k).setExcludeFromIndexes(true).build();
    }

    private static Value dValue(Date k) {
        return makeValue(k).setExcludeFromIndexes(true).build();
    }

    private static Value iValue(int k) {
        return makeValue(k).setExcludeFromIndexes(true).build();
    }

    private static Value gValue(LatLng k) {
        return makeValue(k).setExcludeFromIndexes(true).build();
    }

    @SuppressWarnings("serial")
    public static void main(String[] args) throws TwitterException {
        // 1) Pipeline settings
        String project_id = (System.getProperty("XGCPID") != null) ? (System.getProperty("XGCPID")) : "";
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        // 2) Twitter settings
        Twitter tw = TwitterFactory.getSingleton();
        Query q = new Query("from:@gunken000");
        int num_tweets = 10;
        q.setCount(num_tweets);
        QueryResult result = tw.search(q);
        final List<Status> tweets = result.getTweets();

        PTransform<PCollection<Entity>, ?> write =
                DatastoreIO.v1().write().withProjectId(project_id);

        p.
            apply("GetInMemory", Create.of(tweets)).
            apply("Proc1", ParDo.of(new DoFn<Status, Entity>(){
                @ProcessElement
                public void processElement(ProcessContext c) {
                    Status tweet = c.element();
                    Key.Builder key = makeKey("Tweets", String.valueOf(tweet.getId()));  // k1 is a kind info
                    final Builder builder = Entity.newBuilder().
                            setKey(key).
                            putProperties("user_name", sValue(tweet.getUser().getScreenName())).
                            putProperties("text", sValue(tweet.getText())).
                            putProperties("retweet_count", iValue(tweet.getRetweetCount())).
                            putProperties("favorite_count", iValue(tweet.getFavoriteCount())).
                            putProperties("is_retweet", bValue(tweet.isRetweet())).
                            putProperties("timestamp", dValue(tweet.getCreatedAt()));
                    GeoLocation loc = tweet.getGeoLocation();
                    if (loc != null) {
                        LatLng x = LatLng.
                                newBuilder().
                                setLatitude(loc.getLatitude()).
                                setLongitude(loc.getLongitude()).
                                build();
                        builder.
                        putProperties("geo_point", gValue(x)).
                        putProperties("place", sValue(tweet.getPlace().getFullName())); // tr.set("place", tweet.getPlace().getFullName());
                        System.out.println(tweet.getPlace().getFullName());
                    }
                    MediaEntity[] M = tweet.getMediaEntities();
                    List<String> urls = new ArrayList<String>();
                    for (MediaEntity m : M) {
                        String url = m.getMediaURL()
                                .replace("http://pbs.twimg.com/media/", "")
                                .replace("http://pbs.twimg.com/ext_tw_video_thumb/", "");
                        urls.add(url);
                    }
                    if (urls.size() > 0) {
                        builder.putProperties("media_entities", sValue(String.join(",", urls)));
                    }
                    final Entity entity = builder.build();
                    c.output(entity);
                }
            })).
            apply(write);
        p.run();
    }
}