package kafka.stream.windowing.patient;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.windowing.model.Patient;
import kafka.stream.windowing.model.PatientWithSickRoom;
import kafka.stream.windowing.model.SickRoom;

public class PatientHeartBeatMonitorStream {

	public static void main(String[] args) {
	   StreamsBuilder builder = KafkaHelper.streamBuilderwithStore();

       KStream<String, Long> heartbeatKS = builder.stream(KafkaHelper.HEART_BEAT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("heartbeat-source")
               .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        // 病人做分组根据PatientID
       .groupBy((nokey, patient) -> patient, Grouped.with(Serdes.String(), Serdes.String()))
        // 使用Tumbling window连续监测 
       .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(1)))
        // group内的aggregate 操作
       .count(Named.as("heartbeat-aggregate"), Materialized.as("heartbeat-state-store"))
        // 让window关闭的时候再去往下游发 
       .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
       .toStream() 
       .peek((k, v) -> KafkaHelper.info("pre filtering" + k + '-' + v)) // 现在的key是window key
        //心跳高过80 就需要注意了！
       .filter((k, v) -> v > 80) 
       .peek((k, v) -> KafkaHelper.info("post filtering" + k + '-' + v))
       .selectKey((k, v) -> k.key()); // window key不能用，因此选一个key,即patientID
	   
       // patient 数据是从DB里面拿的，因为这个数据是静态的 不是event driven的 因此要拿所有的数据：earliest
       KTable<String, Patient> patientKTable = builder.table(KafkaHelper.PATIENT_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.PatientSerde())
               .withName("patient-source").withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
	   // 将病房信息也从DB拿出来
       KTable<String, SickRoom> sickRoomKTable = builder.table(KafkaHelper.SICK_ROOM_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.SickRoomSerde())
               .withName("sickroom-source").withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
	   
       // 将 patientKTable 与 sickRoomKTable 进行外键关联，通过SickRoomID.
       KTable<String, PatientWithSickRoom> patientWithSickRoomKTable = patientKTable.join(sickRoomKTable, Patient::getSickRoomID, PatientWithSickRoom::new,
               Materialized.<String, PatientWithSickRoom, KeyValueStore<Bytes, byte[]>>as("patient-sickroom")
                       .withValueSerde(JsonSerdes.PatientWithSickRoomSerde()));
       
       // 将心跳Stream与上面的Ktable进行Join,Stream Join Ktable是不需要window的
       heartbeatKS.join(patientWithSickRoomKTable, (h, p) -> {
           p.setHeartBeat(h);
           return p;
       }, Joined.with(Serdes.String(), Serdes.Long(), JsonSerdes.PatientWithSickRoomSerde()))
       .peek((k, v) -> KafkaHelper.info("Joined" + k + '-' + v))
        // 是否发Alert给主治医生
       .filter((k, v) -> v.getHeartBeat() < 100 ? v.getPatient().getPatientAge() > 25 : true)
       .print(Printed.<String, PatientWithSickRoom>toSysOut().withLabel("hc-monitor-warning"));
		
       KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.WINDOW_PATIENT_APP_ID)));
	}
}
