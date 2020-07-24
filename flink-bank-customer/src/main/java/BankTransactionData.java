

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

public class BankTransactionData {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<String> file = env.readTextFile("src/main/resources/bank_data.csv");
		

		file.flatMap(new ExtractTxnAmount())
		.groupBy(0,2)
		.sum(1)
		.filter(new FilterFunction< Tuple3<StringValue,LongValue,StringValue>>() {
			private static final long serialVersionUID = 1L;
			
		
			public boolean filter(Tuple3<StringValue,LongValue,StringValue> value) throws Exception {
				return value.f1.getValue()>1000000000;
				
			}
		})
		.print();
	}

private static class ExtractTxnAmount implements FlatMapFunction<String,  Tuple3<StringValue,LongValue,StringValue>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	StringValue accNoValue = new StringValue();
    LongValue txnAmtValue =new LongValue();
    StringValue txnDate = new StringValue();
 
   
    Tuple3<StringValue, LongValue,StringValue> result = new Tuple3<StringValue,LongValue,StringValue>(accNoValue, txnAmtValue,txnDate );

   
    public void flatMap(String s, Collector< Tuple3<StringValue, LongValue,StringValue>> collector) throws Exception {
        
        String[] split = s.split(",");
        String accNoStr = split[0];
        String txnAmtStr = split[5];
        String txnDate1 = split[1];
        
        if (!accNoStr.equals("account_no") || !txnAmtStr.equals("amount") || !txnDate1.equals("date")  ) {
        	String  accNo = split[0];
        	long  amount = (int) Double.parseDouble(split[5]);
        	String to_datetxnDate1 = split[1];
        	

        	accNoValue.setValue(accNo);
        	txnAmtValue.setValue(amount);

        	DateFormat format = new SimpleDateFormat("YYYY-MM-DD HH:MM:SS"); 
        	
        	Date txnDate2 = format.parse(to_datetxnDate1);
        	
        	txnDate.setValue(txnDate2.toString().substring(4, 7)+" "+txnDate2.toString().substring(24, 28));
        	
        	collector.collect(result);

        	
        }
    }
}
}