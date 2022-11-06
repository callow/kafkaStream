package kafka.stream.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.ktable.model.Employee;

public class BasicOperation {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithStore();
		
        KTable<String, Employee> table = builder.table(KafkaHelper.EMP_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.EmployeeSerde()).withName("source")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        table.filter((key, value) -> {
        	KafkaHelper.info("filter===>key:" + key + " value:" + value );
            return value.getDepartment().equalsIgnoreCase("Data&Cloud");
        }).filterNot((key, value) -> {
        	KafkaHelper.info("filterNot===>key:" + key + " value:" + value );
            return value.getAge() > 65;
        }).mapValues(employee -> {
        	// 如果之前的值被filter掉了也会打印 null (tombstone)
        	KafkaHelper.info("fmapValues===>value:" + employee );
            return Employee.newBuilder(employee).evaluateTitle().build();
        }).toStream()
        .to(KafkaHelper.EMP_TARGET_TOPIC, Produced.with(Serdes.String(), JsonSerdes.EmployeeSerde()));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.KTABLE_BASIC_APP_ID)));

	}
}
