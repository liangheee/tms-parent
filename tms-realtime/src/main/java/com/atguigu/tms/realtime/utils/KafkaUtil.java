package com.atguigu.tms.realtime.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Hliang
 * @create 2023-09-24 14:58
 */
public class KafkaUtil {
    public static final String bootstrapServers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    /**
     * 创建kafkaSink
     * @param args 动态配置参数
     * @param topic 写入kfaka主题名称
     * @param transactionIdPrefix 作为生产者，保证事务的事务前缀
     * @return 返回 KafkaSink<String>
     */
    public static KafkaSink<String> createKafkaSink(String[] args,String topic, String transactionIdPrefix){
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String servers = parameterTool.get("bootstrap-servers", bootstrapServers);
        topic = parameterTool.get("topic", topic);

        // 如果topic为空，则抛出异常
        if(topic == null || "".equals(topic)){
            throw new RuntimeException("topic主题不能为空！");
        }

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(servers)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchemaBuilder<String>()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix(transactionIdPrefix)
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 15 * 60 + " ")
                .build();
        return kafkaSink;
    }

    /**
     *  创建kafkaSink
     * @param args 动态配置参数
     * @param topic 写入kfaka主题名称
     * @return 返回 KafkaSink<String>
     */
    public static KafkaSink<String> createKafkaSink(String[] args,String topic){
      return createKafkaSink(args,topic,topic + "_trans");
    }

    public static KafkaSource<String> createKafkaSource(String[] args,String topic,String groupId){
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String servers = parameterTool.get("bootstrap-servers", bootstrapServers);

        topic = parameterTool.get("topic",topic);
        if(StringUtils.isEmpty(topic)){
            throw new RuntimeException("Kafka消费者主题不能为空！");
        }

        /*
         * SimpleStringSchema()反序列化器：仅仅支持对非空数据进行反序列化，如果遇到null值会报错
         * 这里我们需要自定义反序列化器
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(servers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null && message.length > 0) {
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();
        return kafkaSource;
    }

    public static KafkaSource<String> createKafkaSource(String[] args,String topic){
        return createKafkaSource(args,topic,"kafka_consumer");
    }
}
