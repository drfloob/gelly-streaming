package org.apache.flink.graph.streaming.example.test;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.graph.streaming.library.ConnectedComponents;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Assert;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WindowConnectedComponentsTest extends StreamingProgramTestBase {
    public static final String Connected_RESULT =
	"{1=[1, 2, 3]}\n"
	+ "{1=[1, 2, 3, 5]}\n"
	+"{1=[1, 5], 6=[6, 7]}\n"
	+"{6=[6, 7], 8=[8, 9]}\n"
	+"{8=[8, 9]}";
    protected String resultPath;
    
    @SuppressWarnings("serial")
    private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) {
	return env.fromCollection(getEdges());
    }

    public static final List<Edge<Long, NullValue>> getEdges() {
	List<Edge<Long, NullValue>> edges = new ArrayList<>();
	edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
	edges.add(new Edge<>(1L, 3L, NullValue.getInstance()));
	edges.add(new Edge<>(2L, 3L, NullValue.getInstance()));
	edges.add(new Edge<>(1L, 5L, NullValue.getInstance()));
	edges.add(new Edge<>(6L, 7L, NullValue.getInstance()));
	edges.add(new Edge<>(8L, 9L, NullValue.getInstance()));
	return edges;
    }

    public static final String[] parser(ArrayList<String> list) {
	int s = list.size();
	String r = list.get(s - 1);  // to get the final combine result which is stored at the end of result
	String t;
	list.clear();
	String[] G = r.split("=");
	for (int i = 0; i < G.length; i++) {
	    if (G[i].contains("[")) {
		String[] k = G[i].split("]");
		t = k[0].substring(1, k[0].length());
		list.add(t);
	    }
	}
	String[] result = list.toArray(new String[list.size()]);
	Arrays.sort(result);
	return result;
    }

    @Override
    protected void preSubmit() throws Exception {
	setParallelism(1); //needed to ensure total ordering for windows
	resultPath = getTempDirPath("output");
    }

    @Override
    protected void postSubmit() throws Exception {
	String expectedResultStr = Connected_RESULT;
	String[] excludePrefixes = new String[0];
	ArrayList<String> list = new ArrayList<String>();
	readAllResultLines(list, resultPath, excludePrefixes, false);
	String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");
	ArrayList<String> expectedArr = new ArrayList<String>(Arrays.asList(expected));
	Assert.assertEquals(expectedArr, list);
	// Assert.assertEquals("Different number of lines in expected and obtained result.", expectedArr.size(), list.size());
    }

    @Override
    protected void testProgram() throws Exception {
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

	DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
	// GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges, env);

	TypeInformation<Tuple2<Integer, Edge<Long, NullValue>>> typeInfo = new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, edges.getType());
	edges
	    .assignTimestampsAndWatermarks(new TargetAssigner())
	    .map(new InitialMapper<Long, NullValue>())
	    .returns(typeInfo)
	    .keyBy(0)
	    .window(SlidingEventTimeWindows.of(Time.milliseconds(4), Time.milliseconds(2)))
	    .fold(new DisjointSet<Long>(),
		  new FoldFunction<Tuple2<Integer, Edge<Long, NullValue>>, DisjointSet<Long>>() {
		    @Override
		    public DisjointSet<Long> fold(DisjointSet<Long> ds, Tuple2<Integer, Edge<Long, NullValue>> val)
			throws Exception {
			ds.union(val.f1.getSource(), val.f1.getTarget());
			return ds;
		    }
		})
	    .map(new MapFunction<DisjointSet<Long>, String>() {
		    @Override
		    public String map(DisjointSet<Long> ds) {
			return ds.toString();
		    }
		})
	    .writeAsText(resultPath);
	
	env.execute("Streaming Connected ComponentsCheck");
    }

    private static final class InitialMapper<K, EV> extends RichMapFunction<Edge<K, EV>, Tuple2<Integer, Edge<K, EV>>> {

	private int partitionIndex;

	@Override
	public void open(Configuration parameters) throws Exception {
	    this.partitionIndex = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public Tuple2<Integer, Edge<K, EV>> map(Edge<K, EV> edge) throws Exception {
	    return new Tuple2<>(partitionIndex, edge);
	}
    }

    private static final class TargetAssigner implements AssignerWithPeriodicWatermarks<Edge<Long, NullValue>> {

	private long currentMaxTimestamp = 1;
	
	public long extractTimestamp(Edge<Long, NullValue> e, long previes) {
	    long timestamp = e.getTarget();
	    currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
	    return timestamp;
	}
	
	public Watermark getCurrentWatermark() {
	    return new Watermark(currentMaxTimestamp);
	}
    }

}

