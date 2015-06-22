package io.norberg.streamsummary;

public interface Distribution {

  /**
   * Estimate the probability of observing the top {@code k} value.
   */
  double probability(int k);
}
