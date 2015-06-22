package io.norberg.streamsummary;

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
public final class StreamSummary<T> {

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

  private static final class Counter<T> {

    private T value;
    private long count;
    private long error;
    private Counter<T> prev;
    private Counter<T> next;

    public Counter(final T value, final long count, final long error) {
      this.value = requireNonNull(value, "value");
      this.count = count;
      this.error = error;
    }

    public Counter(final T value) {
      this(value, 1, 0);
    }

    @Override
    public String toString() {
      return "Counter{" +
             "value=" + value +
             ", count=" + count +
             ", error=" + error +
             '}';
    }

    public Element<T> element() {
      return Element.of(value, count, error);
    }
  }

  public static <T> Element<T> element(final T value, final long count, final long error) {
    return Element.of(value, count, error);
  }

  public static <T> Element<T> element(final T value, final long count) {
    return Element.of(value, count, 0);
  }

  private final int capacity;

  private Counter<T> head;
  private Counter<T> tail;

  private final Map<T, Counter<T>> counters;

  private int size = 0;

  public StreamSummary(final int entries) {
    this.capacity = entries;
    this.counters = new HashMap<>(entries);
  }

  public long record(final T element) {

    // If tracked element, increase counter and promote element.
    Counter<T> counter = counters.get(element);
    if (counter != null) {
      counter.count++;
      if (counter.prev != null && counter.count > counter.prev.count) {
        remove(counter);
        insert(counter, counter.prev);
      }
      return counter.count;
    }

    // If new element and the list of counters is not full, append counter and element at end.
    if (size < capacity) {
      counter = new Counter<>(element);
      if (head == null) {
        head = counter;
      } else {
        tail.next = counter;
        counter.prev = tail;
      }
      tail = counter;
      counters.put(element, counter);
      size++;
      return 1;
    }

    // New element, replace the min counter.
    counter = tail;
    counters.remove(counter.value);
    counters.put(element, counter);
    counter.value = element;
    counter.error = counter.count;
    counter.count++;

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

  public List<Element<T>> top(final int i) {
    return new TopList(i);
  }

  public List<Element<T>> elements() {
    return new TopList(size);
  }

  public int size() {
    return size;
  }

  private class TopList extends AbstractSequentialList<Element<T>> {

    private final int size;

    public TopList(final int size) {
      if (size > StreamSummary.this.size) {
        throw new IndexOutOfBoundsException();
      }
      this.size = size;
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
        return next != null && i < size();
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
        return next.prev != null && i > 0;
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
}
