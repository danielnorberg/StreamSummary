package io.norberg.streamsummary;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class StreamSummary<T> {

  private final long[] counts;
  private final long[] errors;
  private final Object[] elements;

  private int size = 0;

  public StreamSummary(final int entries) {
    this.counts = new long[entries];
    this.errors = new long[entries];
    this.elements = new Object[entries];
  }

  public long inc(final T element) {

    // If tracked element, increase counter and promote element.
    for (int i = 0; i < size; i++) {
      if (Objects.equals(element, elements[i])) {
        final long newCount = counts[i] + 1;
        final int next = i - 1;
        if (i == 0 || newCount <= counts[next]) {
          counts[i] = newCount;
          return newCount;
        }
        demote(next);
        return promote(element, newCount, next, errors[i]);
      }
    }

    // If new element and the list of counters is not full, append counter and element at end.
    if (size < elements.length) {
      elements[size] = element;
      counts[size] = 1;
      size++;
      return 1;
    }

    final int tail = elements.length - 1;
    final int next = tail - 1;

    // New element, replace the min element.
    elements[tail] = element;
    errors[tail] = counts[tail];
    final long newCount = counts[tail] + 1;

    // If new count is not greater then the next count, store and return the new count.
    if (newCount <= counts[next]) {
      counts[tail] = newCount;
      return newCount;
    }

    // New count is greater than the next count, promote the element.
    return promote(element, newCount, tail, errors[tail]);
  }

  private long promote(final T element, final long count, final int index, final long error) {
    int newIndex = index;
    int nextIndex;
    while (newIndex > 0 && count > counts[nextIndex = newIndex - 1]) {
      demote(nextIndex);
      newIndex = nextIndex;
    }
    elements[newIndex] = element;
    counts[newIndex] = count;
    errors[newIndex] = error;
    return count;
  }

  private void demote(final int index) {
    final Object element = elements[index];
    int newIndex = index + 1;
    elements[newIndex] = element;
    counts[newIndex] = counts[index];
    errors[newIndex] = errors[index];
  }

  @SuppressWarnings("unchecked")
  public T element(final int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException();
    }
    return (T) elements[i];
  }


  public long count(final int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException();
    }
    return counts[i];
  }

  public long error(final int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException();
    }
    return errors[i];
  }

  public int size() {
    return size;
  }

  public List<String> table() {
    final List<String> table = new ArrayList<>(size());
    for (int i = 0; i < size; i++) {
      table.add("#" + i + ": " + element(i) + ": " + count(i) + " (e: " + error(i) + ")");
    }
    return table;
  }
}
