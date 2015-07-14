package io.norberg.streamsummary;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.junit.Test;

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

}