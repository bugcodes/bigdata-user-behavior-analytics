package com.bigdata.etl.pipeline.sink;

import com.bigdata.etl.common.serde.JsonSerde;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 本地 JSON Lines 写入器，负责在无 Doris 环境时保存实时聚合结果。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class JsonLineFileSink<T> extends RichSinkFunction<T> {

    private final String outputPath;
    private transient BufferedWriter writer;

    public JsonLineFileSink(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        Path path = Path.of(outputPath);
        Files.createDirectories(path.getParent());
        writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @Override
    public synchronized void invoke(T value, Context context) throws Exception {
        writer.write(JsonSerde.toJson(value));
        writer.newLine();
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}
