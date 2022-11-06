package kafka.stream.stateful.sales;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.stateful.sales.model.Sales;

public class SalesChampionStream {

	public static void main(String[] args) {
		
        //accumulate
        StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
        builder.stream(KafkaHelper.SALES_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.SalesSerde())
              .withName("sales-source").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
              // 
        	  .mapValues(Sales::populateTotalAmount, Named.as("populate-transform"))
              // 按照员工名字分组
              .groupBy((k, v) -> v.getUserName(), Grouped.with(Serdes.String(), JsonSerdes.SalesSerde()))
              // 将他们各自的销售额相加(累计) -> Ktable
              .reduce((aggrValue, currValue) -> Sales.newBuild(currValue).accumulateSalesAmount(aggrValue.getTotalSalesAmount()).build()
                        , Named.as("accumulate-sales-amount"), Materialized.as("accumulate-sales"))
              .toStream()
              // 按照部门分组
              .groupBy((k, v) -> v.getDepartment(), Grouped.with(Serdes.String(), JsonSerdes.SalesSerde()))
              // 选出部门冠军
              .reduce((aggValue, currValue) -> currValue.getTotalSalesAmount() > aggValue.getTotalSalesAmount() ? currValue : aggValue,
                        Named.as("champion-sales"), Materialized.as("sales-champion"))
              .toStream()
              .print(Printed.<String, Sales>toSysOut().withLabel("sales-champion"));
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATEFUL_REDUCE_SALE_CHAMPION_APP_ID)));

	}
}
