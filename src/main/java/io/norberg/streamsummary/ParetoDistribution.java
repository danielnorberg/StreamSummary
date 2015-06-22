package io.norberg.streamsummary;

public final class ParetoDistribution implements Distribution {

  private final double scale;
  private final double shape;

  private ParetoDistribution(final double scale, final double shape) {
    this.scale = scale;
    this.shape = shape;
  }

  public static ParetoDistribution of(final double scale, final double shape) {
    return new ParetoDistribution(scale, shape);
  }


  @Override
  public double probability(final int k) {
    return cumulative(scale + 1 + k) - cumulative(scale + k);
  }

  private double cumulative(final double x) {
    return 1 - Math.pow(scale / x, shape);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ParetoDistribution that = (ParetoDistribution) o;

    if (Double.compare(that.scale, scale) != 0) {
      return false;
    }
    return Double.compare(that.shape, shape) == 0;

  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(scale);
    result = (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(shape);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "ParetoDistribution{" +
           "scale=" + scale +
           ", shape=" + shape +
           '}';
  }
}
