package kafka.stream.stateful.sales;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.stateful.sales.model.SalesStats;

public class SalesAggregateStream {

	public static void main(String[] args) {
		   StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
	       builder.stream(KafkaHelper.SALES_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.SalesSerde()).withName("source-processor").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .selectKey((k, v) -> v.getDepartment())
                 // 每个department放入不同的partition
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.SalesSerde()))
                 // 将Sales转成一个SalesStats， apply() 方法就是aggregate， (k,curV,aggr)
                .aggregate(SalesStats::new, (dept, sales, salesStatsAggr) -> {
                    if (salesStatsAggr.getDepartment() == null) { // 第一次没有值
                        salesStatsAggr.setDepartment(sales.getDepartment());
                        salesStatsAggr.setCount(1);
                        salesStatsAggr.setTotalAmount(sales.getSalesAmount());
                        salesStatsAggr.setAverageAmount(sales.getSalesAmount());
                    } else { // 以后有值了
                        salesStatsAggr.setCount(salesStatsAggr.getCount() + 1);
                        salesStatsAggr.setTotalAmount(salesStatsAggr.getTotalAmount() + sales.getSalesAmount());
                        salesStatsAggr.setAverageAmount(salesStatsAggr.getTotalAmount() / salesStatsAggr.getCount());
                    }
                    return salesStatsAggr;
                }, Named.as("aggregate-process"), // 持久化到state store,因此现在类型发生了变化，所以改变Serde
                        Materialized.<String, SalesStats, KeyValueStore<Bytes, byte[]>>as("sales-stats")
                        .withKeySerde(Serdes.String()).withValueSerde(JsonSerdes.SalesStatsSerde()))
                .toStream()
                .print(Printed.<String, SalesStats>toSysOut().withLabel("sales-stats"));
	       
	        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATEFUL_AGGREDATE_SALE_CHAMPION_APP_ID)));
	}
}
