package io.norberg.streamsummary;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static io.norberg.streamsummary.StreamSummary.element;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class StreamSummaryTest {


  @Test
  public void testSmallCount() {
    final StreamSummary<String> sut = StreamSummary.of(5);
    assertThat(sut.record("a"), is(1L));
    assertThat(sut.record("a"), is(2L));
    assertThat(sut.record("b"), is(1L));
    assertThat(sut.record("c"), is(1L));

    assertThat(sut.elements().stream().collect(toList()),
               is(asList(element("a", 2L), element("b", 1L), element("c", 1L))));

    assertThat(sut.record("a"), is(3L));

    assertThat(sut.record("c"), is(2L));
    assertThat(sut.record("c"), is(3L));
    assertThat(sut.record("c"), is(4L));

    assertThat(sut.elements().stream().collect(toList()),
               is(asList(element("c", 4L), element("a", 3L), element("b", 1L))));
  }

  @Test
  public void testPareto_50_0p5_200M() {
    final ParetoDistribution distribution = new ParetoDistribution(50, 0.5);

    final int n = 200000000;

    final StreamSummary<Integer> sut = StreamSummary.<Integer>builder()
        .paretoDistribution(50, 0.5)
        .observations(n)
        .top(10)
        .build();

    for (int i = 0; i < n; i++) {
      final int element = (int) distribution.sample();
      sut.record(element);
    }

    sut.top(10).forEach(System.out::println);
  }

  @Test
  public void testNormal_10M() {
    final int n = 10000000;
    final NormalDistribution distribution = new NormalDistribution(0, 10);


    final StreamSummary<Integer> sut = StreamSummary.<Integer>builder()
        .distribution(k -> distribution.probability(k, k + 1))
        .observations(n)
        .top(10)
        .build();

    for (int i = 0; i < n; i++) {
      final int element = Math.abs((int) distribution.sample());
      sut.record(element);
    }

    sut.top(10).forEach(System.out::println);
  }

  @Test
  public void mergeTest() {
    final StreamSummary<Integer> s1 = pareto(10, 1_000_000, 50, 0.1);
    final StreamSummary<Integer> s2 = pareto(10, 1_000_000, 50, 0.1);

    s1.merge(s2);

    s1.top(10).forEach(System.out::println);

    assertThat(s1.count(), is(2_000_000L));
  }

  private StreamSummary<Integer> normal(final int k, final int n) {
    final NormalDistribution distribution = new NormalDistribution(0, 10);

    final StreamSummary<Integer> ss = StreamSummary.<Integer>builder()
        .distribution(x -> distribution.probability(x, x + 1))
        .observations(n)
        .top(k)
        .build();

    for (int i = 0; i < n; i++) {
      final int element = Math.abs((int) distribution.sample());
      ss.record(element);
    }

    return ss;
  }

  private StreamSummary<Integer> pareto(final int k, final int n, final double scale, final double shape) {
    final ParetoDistribution distribution = new ParetoDistribution(scale, shape);

    final StreamSummary<Integer> sut = StreamSummary.<Integer>builder()
        .paretoDistribution(scale, shape)
        .observations(n)
        .top(k)
        .build();

    for (int i = 0; i < n; i++) {
      final int element = (int) distribution.sample();
      sut.record(element);
    }

    return sut;
  }

  @Test
  public void testSerialization() throws Exception {
    final StreamSummary<Integer> sut = pareto(10, 1_000_000, 50, 0.1);

    final ByteArrayOutputStream b = new ByteArrayOutputStream();
    final ObjectOutputStream os = new ObjectOutputStream(b);
    os.writeObject(sut);

    final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(b.toByteArray()));

    @SuppressWarnings("unchecked") final StreamSummary<Integer> deserialized =
        (StreamSummary<Integer>) is.readObject();

    assertThat(deserialized.elements(), is(sut.elements()));
    assertThat(deserialized.count(), is(sut.count()));
    assertThat(deserialized.capacity(), is(sut.capacity()));
    assertThat(deserialized.size(), is(sut.size()));
  }

  static class Foo {

    private static final io.norberg.streamsummary.ParetoDistribution
        TRACK_DISTRIBUTION = io.norberg.streamsummary.ParetoDistribution.of(50, 0.1);

    private static final long TRACKS_PER_SECOND = 30 * 1000;
    private static final long WINDOW_LENGTH = 10 * 60;
    private static final long EXPECTED_OBSERVATIONS = WINDOW_LENGTH * TRACKS_PER_SECOND;


    public static void main(final String... args) throws IOException {
      final StreamSummary<Integer> sut = StreamSummary.<Integer>builder()
          .paretoDistribution(50, 0.1)
          .observations(EXPECTED_OBSERVATIONS)
          .top(50)
          .build();

      System.out.println(sut.capacity());

      for (int i = 0; i < 10000; i++) {
        sut.record(i);
      }

      final ByteArrayOutputStream b = new ByteArrayOutputStream();
      final ObjectOutputStream s = new ObjectOutputStream(b);
      s.writeObject(sut);

      System.out.println(b.toByteArray().length);

    }
  }

}