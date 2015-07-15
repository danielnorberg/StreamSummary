package io.norberg.streamsummary;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.AbstractSequentialList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * An implementation of the StreamSummary algorithm described in <i>Efficient Computation of
 * Frequent and Top-k Elements in Data Streams</i> by Metwally, Agrawal and Abbadi.
 *
 * <a href="https://icmi.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf">
 * https://icmi.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf</a>
 */
public final class StreamSummary<T> implements Serializable {

  private static final long serialVersionUID = 0;

  private final int capacity;

  transient private Map<T, Counter<T>> counters;

  transient private Counter<T> head;
  transient private Counter<T> tail;

  private long count;

  private int size;

  private StreamSummary(final int entries) {
    this.capacity = entries;
    this.counters = new HashMap<>(entries);
  }

  /**
   * Record a single observation of an element.
   */
  public long record(final T element) {
    return record(element, 1);
  }

  /**
   * Record {@code count} observations of an element.
   */
  public long record(final T element, final long count) {
    return record(element, count, 0);
  }

  /**
   * Merge another {@link StreamSummary} into this summary.
   */
  public void merge(final StreamSummary<T> other) {
    for (final Element<T> element : other.elements()) {
      record(element.value(), element.count(), element.error());
    }
  }

  /**
   * The top {@code i} elements, in descending count order, bounded by the current {@link #size()}.
   */
  public List<Element<T>> top(final int i) {
    return new TopList(i);
  }

  /**
   * A list of the top {@link #size()} elements, in descending count order.
   */
  public List<Element<T>> elements() {
    return new TopList(size);
  }

  /**
   * The maximum number of top elements tracked by this summary.
   */
  public int capacity() {
    return capacity;
  }

  /**
   * The number of top elements in this summary, bounded by {@link #capacity()}.
   */
  public int size() {
    return size;
  }

  /**
   * The total element count recorded in this summary.
   */
  public long count() {
    return count;
  }

  private long record(final T element, final long count, final long error) {

    // Increase total count
    this.count += count;

    // If tracked element, increase counter and promote element.
    Counter<T> counter = counters.get(element);
    if (counter != null) {
      counter.count += count;
      counter.error += error;
      if (counter.prev != null && counter.count > counter.prev.count) {
        remove(counter);
        insert(counter, counter.prev);
      }
      return counter.count;
    }

    // If new element and the list of counters is not full, append counter and element at end.
    if (size < capacity) {
      counter = new Counter<>(element, count, error);
      if (head == null) {
        head = counter;
      } else {
        tail.next = counter;
        counter.prev = tail;
      }
      tail = counter;
      counters.put(element, counter);
      size++;
      return counter.count;
    }

    // New element, replace the min counter.
    counter = tail;
    counters.remove(counter.value);
    counters.put(element, counter);
    counter.value = element;
    counter.error = counter.count;
    counter.count += count;
    counter.error += error;

    if (counter.prev != null && counter.count > counter.prev.count) {
      remove(counter);
      insert(counter, counter.prev);
    }

    return counter.count;
  }

  private void insert(final Counter<T> element, Counter<T> prev) {
    Counter<T> next = prev;
    while (prev != null && element.count > prev.count) {
      next = prev;
      prev = prev.prev;
    }
    element.prev = prev;
    element.next = next;
    next.prev = element;
    if (prev == null) {
      head = element;
    } else {
      prev.next = element;
    }
  }

  private void remove(final Counter<T> element) {
    if (element == head) {
      head = element.next;
    } else {
      element.prev.next = element.next;
    }
    if (element == tail) {
      tail = element.prev;
    } else {
      element.next.prev = element.prev;
    }
  }

  @Override
  public String toString() {
    return "StreamSummary{" +
           "count=" + count +
           ", elements=" + elements() +
           ", capacity=" + capacity +
           ", size=" + size +
           '}';
  }

  public static <T> Element<T> element(final T value, final long count, final long error) {
    return Element.of(value, count, error);
  }

  public static <T> Element<T> element(final T value, final long count) {
    return Element.of(value, count, 0);
  }

  public static <T> StreamSummary<T> of(final int entries) {
    return new StreamSummary<>(entries);
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static final class Builder<T> {

    private Distribution distribution;
    private long observations;
    private int k;
    private Long estimate;

    private Builder() {
    }

    public Builder<T> paretoDistribution(final double scale, final double shape) {
      return distribution(ParetoDistribution.of(scale, shape));
    }

    public Builder<T> distribution(final Distribution distribution) {
      this.distribution = requireNonNull(distribution, "distribution");
      return this;
    }

    public Builder<T> observations(final long n) {
      this.observations = n;
      return this;
    }

    public Builder<T> top(final int k) {
      this.k = k;
      this.estimate = null;
      return this;
    }

    public Builder<T> top(final int k, final long estimate) {
      this.k = k;
      this.estimate = estimate;
      return this;
    }

    public StreamSummary<T> build() {
      final long estimate;
      if (this.estimate != null) {
        estimate = this.estimate;
      } else {
        requireNonNull(distribution, "missing either top k observation estimate or distribution");
        final double probability = distribution.probability(k);
        estimate = (long) (probability * observations);
      }
      if (estimate == 0) {
        throw new IllegalArgumentException("top k observation estimate is zero");
      }
      final int entries = (int) (2 * observations / estimate);
      return StreamSummary.of(entries);
    }
  }

  public static final class Element<T> {

    private T value;
    private long count;
    private long error;

    private Element(final T value, final long count, final long error) {
      this.value = requireNonNull(value, "value");
      this.count = count;
      this.error = error;
    }

    private Element(final T value) {
      this(value, 1, 0);
    }

    public T value() {
      return value;
    }

    public long count() {
      return count;
    }

    public long error() {
      return error;
    }

    public static <T> Element<T> of(final T value, final long count, final long error) {
      return new Element<T>(value, count, error);
    }

    public static <T> Element<T> of(final T value, final long count) {
      return new Element<T>(value, count, 0);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Element<?> element = (Element<?>) o;

      if (count != element.count) {
        return false;
      }
      if (error != element.error) {
        return false;
      }
      return !(value != null ? !value.equals(element.value) : element.value != null);

    }

    @Override
    public int hashCode() {
      int result = value != null ? value.hashCode() : 0;
      result = 31 * result + (int) (count ^ (count >>> 32));
      result = 31 * result + (int) (error ^ (error >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "Element{" +
             "value=" + value +
             ", count=" + count +
             ", error=" + error +
             '}';
    }
  }

  private static final class Counter<T> implements Serializable {

    private static final long serialVersionUID = 0;

    private T value;
    private long count;
    private long error;
    private Counter<T> prev;
    private Counter<T> next;

    private Counter(final T value, final long count, final long error) {
      this.value = requireNonNull(value, "value");
      this.count = count;
      this.error = error;
    }

    @Override
    public String toString() {
      return "Counter{" +
             "value=" + value +
             ", count=" + count +
             ", error=" + error +
             '}';
    }

    private Element<T> element() {
      return Element.of(value, count, error);
    }
  }

  private class TopList extends AbstractSequentialList<Element<T>> {

    private final int size;

    public TopList(final int size) {
      if (size > capacity) {
        throw new IndexOutOfBoundsException();
      }
      this.size = Math.min(StreamSummary.this.size, size);
    }

    @Override
    public ListIterator<Element<T>> listIterator(final int index) {
      return new Iterator();
    }

    @Override
    public int size() {
      return size;
    }

    private class Iterator implements ListIterator<Element<T>> {

      Counter<T> next = head;
      int i;

      @Override
      public boolean hasNext() {
        return i < size;
      }

      @Override
      public Element<T> next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        final Counter<T> next = this.next;
        this.next = next.next;
        i++;
        return next.element();
      }

      @Override
      public boolean hasPrevious() {
        return i > 0;
      }

      @Override
      public Element<T> previous() {
        if (!hasPrevious()) {
          throw new IllegalStateException();
        }
        final Counter<T> prev = next.prev;
        this.next = next.prev;
        i--;
        return prev.element();
      }

      @Override
      public int nextIndex() {
        return i;
      }

      @Override
      public int previousIndex() {
        return i - 1;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void set(final Element<T> element) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void add(final Element<T> element) {
        throw new UnsupportedOperationException();
      }
    }
  }

  private void writeObject(java.io.ObjectOutputStream s)
      throws java.io.IOException {
    // Write serialization magic and primitive fields
    s.defaultWriteObject();

    // Write out all counters in order.
    for (Counter<T> c = head; c != null; c = c.next) {
      s.writeObject(c.value);
      s.writeLong(c.count);
      s.writeLong(c.error);
    }
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream s)
      throws java.io.IOException, ClassNotFoundException {
    // Read serialization magic and primitive fields
    s.defaultReadObject();

    // Read in all counters in order.
    head = readCounter(s);
    Counter<T> curr = head;
    Counter<T> prev;
    for (int i = 1; i < size; i++) {
      prev = curr;
      curr = readCounter(s);
      prev.next = curr;
      curr.prev = prev;
    }
    tail = curr;

    // Recreate counter map
    counters = new HashMap<>();
    for (Counter<T> c = head; c != null; c = c.next) {
      counters.put(c.value, c);
    }
  }

  private Counter<T> readCounter(final ObjectInputStream s) throws IOException, ClassNotFoundException {
    @SuppressWarnings("unchecked") final T value = (T) s.readObject();
    final long count = s.readLong();
    final long error = s.readLong();
    return new Counter<>(value, count, error);
  }

}
