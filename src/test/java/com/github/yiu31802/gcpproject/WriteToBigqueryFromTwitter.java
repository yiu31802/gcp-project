package com.github.yiu31802.gcpproject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import twitter4j.GeoLocation;
import twitter4j.MediaEntity;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class WriteToBigqueryFromTwitter {
    @SuppressWarnings("serial")
    static final SerializableFunction<TableRow, TableRow> IDENTITY_FORMATTER = new SerializableFunction<TableRow, TableRow>() {
        @Override
        public TableRow apply(TableRow input) {
            return input;
        }
    };

    private static TableSchema schemaGen() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("text").setType("STRING"));
        fields.add(new TableFieldSchema().setName("user_id").setType("STRING"));
        fields.add(
                new TableFieldSchema().setName("user_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("timestamp")
                .setType("timestamp"));
        fields.add(new TableFieldSchema().setName("place").setType("STRING"));
        fields.add(new TableFieldSchema().setName("geo_location")
                .setType("STRING"));
        fields.add(new TableFieldSchema().setName("retweet_count")
                .setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("favorite_count")
                .setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("media_entities")
                .setType("STRING"));
        fields.add(new TableFieldSchema().setName("is_retweet")
                .setType("BOOLEAN"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    @SuppressWarnings("serial")
    public static void main(String[] args) throws TwitterException {
        String project_id = (System.getProperty("XGCPID") != null)
                ? (System.getProperty("XGCPID"))
                : "";
        TableSchema schema = schemaGen();
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().create();
        GcpOptions gOptions = options.as(GcpOptions.class);
        gOptions.setProject(project_id);
        String gcp_temp = "gs://" + project_id + ".appspot.com/bigquery-tmp";
        gOptions.setGcpTempLocation(gcp_temp);
        gOptions.setTempLocation(gcp_temp);
        Pipeline p = Pipeline.create(gOptions);
        PTransform<PCollection<TableRow>, ?> write = BigQueryIO
                .<TableRow> write().withFormatFunction(IDENTITY_FORMATTER)
                .to("ds1.tweet").withSchema(schema)
                .withWriteDisposition(
                        BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(
                        BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

        Twitter tw = TwitterFactory.getSingleton();
        Query q = new Query("from:@gunken000");
        int num_tweets = 5;
        q.setCount(num_tweets);
        // q.setGeoCode(new GeoLocation(35.652832, 139.839478), 8000,
        // Query.KILOMETERS);
        QueryResult result = tw.search(q);
        final List<Status> tweets = result.getTweets();
        List<Integer> ids = IntStream.range(0, num_tweets).boxed()
                .collect(Collectors.toList());
        System.out.println(ids);

        p.apply("GetInMemory", Create.of(ids))
                .apply("Proc1", ParDo.of(new DoFn<Integer, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        final TableRow tr = new TableRow();
                        Status tweet = tweets.get(c.element());
                        System.out.println(tweet.getText());
                        long x = tweet.getCreatedAt().getTime() / 1000;
                        String g = "";
                        tr.set("id", String.valueOf(tweet.getId()))
                                .set("text", tweet.getText())
                                .set("user_id",
                                        String.valueOf(tweet.getUser().getId()))
                                .set("user_name",
                                        tweet.getUser().getScreenName())
                                .set("timestamp", x)
                                .set("retweet_count", tweet.getRetweetCount())
                                .set("favorite_count", tweet.getFavoriteCount())
                                .set("is_retweet", tweet.isRetweet());
                        if (tweet.getPlace() != null) {
                            tr.set("place", tweet.getPlace().getFullName());
                        }
                        GeoLocation loc = tweet.getGeoLocation();
                        if (loc != null) {
                            tr.set("place", tweet.getPlace().getFullName());
                            tr.set("geo_location", String.format("%f, %f",
                                    loc.getLatitude(), loc.getLongitude()));
                        } else {
                            System.out.println("geo is null");
                        }
                        MediaEntity[] M = tweet.getMediaEntities();
                        List<String> urls = new ArrayList<String>();
                        for (MediaEntity m : M) {
                            String url = m.getMediaURL()
                                    .replace("http://pbs.twimg.com/media/", "")
                                    .replace(
                                            "http://pbs.twimg.com/ext_tw_video_thumb/",
                                            "");
                            urls.add(url);
                        }
                        if (urls.size() > 0) {
                            tr.set("media_entities", String.join(",", urls));
                        }
                        c.output(tr);
                    }
                })).apply(write);
        p.run();
    }
}
