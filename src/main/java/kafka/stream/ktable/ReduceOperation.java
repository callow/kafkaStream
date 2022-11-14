package kafka.stream.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.ktable.model.Employee;

public class ReduceOperation {

	/**
	 * 计算部门工资 <br><br>
	 * 
	 * 注： update = delete old + insert new 
	 */
	
	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();

		 KTable<String, Employee> table = builder.table(KafkaHelper.EMP_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.EmployeeSerde()).withName("source")
	                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

		    // 通过部门name/id进行分组
	        table.groupBy((k, v) -> KeyValue.pair(v.getDepartment(), v), Grouped.with(Serdes.String(), JsonSerdes.EmployeeSerde()))
	        // 对一个新employee入职本部门 只会走adder, 对一个原有员工的工资调整，会先走 subtractor删掉旧的 再走  adder  添加新记录    
	        .reduce(//adder 
	                        (currentAgg, newValue) -> {
	                        	KafkaHelper.info("adder--当前已有的:" + currentAgg + " 新增的:" + newValue);
	                            Employee employee = Employee.newBuilder(currentAgg).build();
	                            employee.setTotalSalary(currentAgg.getSalary() + newValue.getSalary());
	                            return employee;
	                        },
	                        //subtractor
	                        (currentAgg, oldValue) -> {
	                        	KafkaHelper.info("subtractor--当前已有的:" + currentAgg + " 旧的:" + oldValue);
	                            Employee employee = Employee.newBuilder(currentAgg).build();
	                            employee.setTotalSalary(currentAgg.getTotalSalary() - oldValue.getSalary());
	                            return employee;
	                        },
	                        Named.as("reducer"),
	                        Materialized.as("Total-Salary-Per-Department")
	                ).toStream()
	                .foreach((k, v) -> KafkaHelper.info("department:" + k + " total salary:" + v.getTotalSalary()));
	        
	        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.KTABLE_REDUCE_APP_ID)));

	}
}
