package io.norberg.streamsummary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class StreamSummary<T> {

  private static final int MIN_ENTRIES = 256;

  private final long[] counts;
  private final long[] errors;
  private final Object[] elements;
  private final Map<Object, Integer> indices;

  private int size = 0;

  private final long[] minCounts = new long[MIN_ENTRIES];
  private final long[] minErrors = new long[MIN_ENTRIES];
  private final Object[] minElements = new Object[MIN_ENTRIES];

  public StreamSummary(final int entries) {
    this.counts = new long[entries];
    this.errors = new long[entries];
    this.elements = new Object[entries];
    this.indices = new HashMap<>(entries);
  }

  public long inc(final T element) {
    final Integer index = indices.get(element);

    // If recorded and not the min element, increase counter and promote element.
    if (index != null) {
      final long newCount = counts[index] + 1;
      final int next = index - 1;
      if (index == 0 || newCount <= counts[next]) {
        counts[index] = newCount;
        return newCount;
      }
      demote(next);
      return promote(element, newCount, next, errors[index]);
    }

    // If new element and the list of counters is not full, append counter and element at end.
    if (size < elements.length) {
      indices.put(element, size);
      elements[size] = element;
      counts[size] = 1;
      size++;
      return 1;
    }

    // If new element and the set of counters is full, add this element to the min set.
    final int tail = counts.length - 1;
    long minCount = Long.MAX_VALUE;
    int minMin = -1;
    for (int i = 0; i < minElements.length; i++) {
      if (element.equals(minElements[i])) {
        final long newCount = minCounts[i] + 1;
        // If new min count is not greater then the next count, store and return the new min count.
        if (newCount <= counts[tail]) {
          minCounts[i] = newCount;
          return newCount;
        }

        // New min count is greater than the tail count, demote the tail element to min and
        // promote the old min element.
        final long error = minErrors[i];
        minCounts[i] = counts[tail];
        minErrors[i] = errors[tail];
        Object tailElement = elements[tail];
        minElements[i] = tailElement;
        indices.remove(tailElement);
        return promote(element, newCount, tail, error);
      }

      if (minCounts[i] < minCount) {
        minCount = minCounts[i];
        minMin = i;
      }
    }

    // Element not found in min set either, replace the min-min.
    minElements[minMin] = element;
    minErrors[minMin] = minCounts[minMin];
    final long newCount = minCounts[minMin] + 1;

    // If new min count is not greater then the next count, store and return the new min count.
    if (newCount <= counts[tail]) {
      minCounts[minMin] = newCount;
      return newCount;
    }

    // New min count is greater than the tail count, demote the tail element to min and
    // promote the old min element.
    final long error = minErrors[minMin];
    minCounts[minMin] = counts[tail];
    minErrors[minMin] = errors[tail];
    Object tailElement = elements[tail];
    minElements[minMin] = tailElement;
    indices.remove(tailElement);
    return promote(element, newCount, tail, error);
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
    indices.put(element, newIndex);
    return count;
  }

  private void demote(final int index) {
    final Object element = elements[index];
    int newIndex = index + 1;
    elements[newIndex] = element;
    counts[newIndex] = counts[index];
    errors[newIndex] = errors[index];
    indices.put(element, newIndex);
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
