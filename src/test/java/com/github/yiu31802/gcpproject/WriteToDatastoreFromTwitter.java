package com.github.yiu31802.gcpproject;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;


public class WriteToDatastoreFromMemory {
    @SuppressWarnings("serial")
    public static void main(String[] args) {
        String project_id = (System.getProperty("XGCPID") != null) ? (System.getProperty("XGCPID")) : "";
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);
        List<String> keyNames = Arrays.asList("L1", "L2");
        PTransform<PCollection<Entity>, ?> write =
                DatastoreIO.v1().write().withProjectId(project_id);

        p.
            apply("GetInMemory", Create.of(keyNames)).setCoder(StringUtf8Coder.of()).
            apply("Proc1", ParDo.of(new DoFn<String, Entity>(){
                @ProcessElement
                public void processElement(ProcessContext c) {
                    Key.Builder key = makeKey("k2", c.element());  // k1 is a kind info
                    final Entity entity = Entity.newBuilder().
                            setKey(key).
                            putProperties("p1", makeValue(new String("test constant value")
                              ).setExcludeFromIndexes(true).build()).
                            build();
                    c.output(entity);
                }
            })).
            apply(write);
        p.run();
    }
}