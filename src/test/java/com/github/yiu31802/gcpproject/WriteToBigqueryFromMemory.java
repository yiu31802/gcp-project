package com.github.yiu31802.gcpproject;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.extensions.gcp.options.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class WriteToBigqueryFromMemory {
    @SuppressWarnings("serial")
    static final SerializableFunction<TableRow, TableRow> IDENTITY_FORMATTER =
            new SerializableFunction<TableRow, TableRow>() {
              @Override
              public TableRow apply(TableRow input) {
                return input;
              }
            };

    private static TableSchema schemaGen() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("name_JP").setType("STRING"));
        fields.add(new TableFieldSchema().setName("city_id").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
      }            
            
    @SuppressWarnings("serial")
    public static void main(String[] args) {
        String project_id = (System.getProperty("XGCPID") != null) ? (System.getProperty("XGCPID")) : "";
        TableSchema schema = schemaGen();
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        GcpOptions gOptions = options.as(GcpOptions.class);
        gOptions.setProject(project_id);
        Pipeline p = Pipeline.create(gOptions);
        List<Integer> ids = Arrays.asList(1, 2);
        PTransform<PCollection<TableRow>, ?> write =
                BigQueryIO.<TableRow>write().
                    withFormatFunction(IDENTITY_FORMATTER).
                    to("ds1.city").
                    withSchema(schema);
        p.
            apply("GetInMemory", Create.of(ids)).
            apply("Proc1", ParDo.of(new DoFn<Integer, TableRow>(){
                @ProcessElement
                public void processElement(ProcessContext c) {
                    final TableRow tr = new TableRow();
                    tr.set("name", "Sapporo").set("name_jp", "札幌").set("city_id", c.element());
                    c.output(tr);
                }
            })).
            apply(write);
        p.run();
    }
}