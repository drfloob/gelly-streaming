package org.apache.flink.graph.streaming.example.test;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.graph.streaming.library.WindowConnectedComponents;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Assert;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WindowConnectedComponentsTest extends StreamingProgramTestBase {
	public static final String Connected_RESULT =
	    "{1=[1, 2, 3]}\n"
	    + "{1=[1, 2, 3, 5]}\n"
	    + "{1=[1, 5], 6=[6, 7]}\n"
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
		Assert.assertEquals("Different number of lines in expected and obtained result.", expectedArr.size(), list.size());
		Assert.assertEquals(expectedArr, list);
	}

	@Override
	protected void testProgram() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(
		        edges,
			new AscendingTimestampExtractor<Edge<Long, NullValue>>(){
			    @Override
			    public long extractAscendingTimestamp(Edge<Long, NullValue> edge) {
				return edge.getTarget();
			    }
			}, env);
		SlidingEventTimeWindows windowAssigner = SlidingEventTimeWindows.of(Time.milliseconds(4), Time.milliseconds(2));
		DataStream<DisjointSet<Long>> cc = graph.aggregate(new WindowConnectedComponents<Long, NullValue>(windowAssigner));
		cc.writeAsText(resultPath);
		env.execute("Streaming Connected ComponentsCheck");
	}
}

