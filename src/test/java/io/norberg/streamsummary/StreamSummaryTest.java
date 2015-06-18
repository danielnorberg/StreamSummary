package io.norberg.streamsummary;

import org.apache.commons.math3.distribution.ParetoDistribution;
import org.junit.Test;

import java.util.BitSet;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class StreamSummaryTest {


  @Test
  public void testSmallCount() {
    final StreamSummary<String> sut = new StreamSummary<>(5);
    assertThat(sut.inc("a"), is(1L));
    assertThat(sut.inc("a"), is(2L));
    assertThat(sut.inc("b"), is(1L));
    assertThat(sut.inc("c"), is(1L));

    assertThat(sut.element(0), is("a"));
    assertThat(sut.count(0), is(2L));

    assertThat(sut.element(1), is("b"));
    assertThat(sut.count(1), is(1L));

    assertThat(sut.element(2), is("c"));
    assertThat(sut.count(2), is(1L));

    assertThat(sut.inc("a"), is(3L));

    assertThat(sut.inc("c"), is(2L));
    assertThat(sut.inc("c"), is(3L));
    assertThat(sut.inc("c"), is(4L));

    assertThat(sut.element(0), is("c"));
    assertThat(sut.count(0), is(4L));

    assertThat(sut.element(1), is("a"));
    assertThat(sut.count(1), is(3L));

    assertThat(sut.element(2), is("b"));
    assertThat(sut.count(2), is(1L));
  }

  @Test
  public void testPareto_50_0p5_200M() {
    final BitSet elements = new BitSet();
    final ParetoDistribution distribution = new ParetoDistribution(50, 0.5);

    final int n = 200000000;

    final double topEstimate = distribution.cumulativeProbability(distribution.getScale() + 1) * n;
    final double nr200Estimate =
        (distribution.cumulativeProbability(distribution.getScale() + 201) -
         distribution.cumulativeProbability(distribution.getScale() + 200)) * n;
    final int entries = (int) (2 * n / topEstimate);
    final StreamSummary<Integer> sut = new StreamSummary<>(entries);

    System.out.println("top estimate: " + (int) topEstimate);
    System.out.println("#200 estimate: " + (int) nr200Estimate);
    System.out.println("stream summary entries: " + entries);

    for (int i = 0; i < n; i++) {
      final int element = (int) distribution.sample();
      elements.set(element);
      sut.inc(element);
    }

    System.out.println("cardinality: " + elements.cardinality());

    sut.table().stream().limit(10).forEach(System.out::println);
  }

}