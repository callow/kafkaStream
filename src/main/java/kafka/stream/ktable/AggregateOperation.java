package kafka.stream.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.ktable.model.Employee;
import kafka.stream.ktable.model.EmployeeStats;

public class AggregateOperation {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();

		KTable<String, Employee> table = builder.table(KafkaHelper.EMP_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.EmployeeSerde()).withName("source")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        table.groupBy((k, v) -> KeyValue.pair(v.getDepartment(), v), Grouped.with(Serdes.String(), JsonSerdes.EmployeeSerde()))
                .aggregate(EmployeeStats::new,

                (key, newValue, aggregate) -> {
	                	KafkaHelper.info("add-- " + key + " " + newValue + " " + aggregate);
	                    if (aggregate.getDepartment() == null) {
	                        aggregate.setDepartment(newValue.getDepartment());
	                        aggregate.setTotalSalary(newValue.getSalary());
	                    } else {
	                        aggregate.setTotalSalary(aggregate.getTotalSalary() + newValue.getSalary());
	                    }
	                    return aggregate;
                 }, 
                		
            	(key, oldValue, aggregate) -> {
            		KafkaHelper.info("sub-- " + key + " " + oldValue + " " + aggregate);
                    aggregate.setTotalSalary(aggregate.getTotalSalary() - oldValue.getSalary());
                    return aggregate;
                 },
                 Named.as("aggregate"),
                 Materialized.<String, EmployeeStats, KeyValueStore<Bytes, byte[]>>as("aggregate").withValueSerde(JsonSerdes.EmployeeStatsSerde())
                ).toStream()
                .print(Printed.<String, EmployeeStats>toSysOut().withLabel("total-salary"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.KTABLE_AGGREGATE_APP_ID)));

	}
}
