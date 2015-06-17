package io.norberg.streamsummary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class StreamSummary<T> {

  private final long[] counts;
  private final long[] errors;
  private final Object[] elements;
  private final Map<Object, Integer> indices;

  private int size = 0;

  private long minCount;
  private long minError;
  private Object minElement;

  public StreamSummary(final int entries) {
    this.counts = new long[entries - 1];
    this.errors = new long[entries - 1];
    this.elements = new Object[entries - 1];
    this.indices = new HashMap<>(entries - 1);
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

    // If new element, the set of counters is full but there is no min element, make this element
    // the min element.
    if (minElement == null) {
      minElement = element;
      minCount = 1;
      size++;
      return 1;
    }

    // If new element and the set of counters is full, replace the min element.
    if (!minElement.equals(element)) {
      minElement = element;
      minError = minCount;
    }
    final long newCount = minCount + 1;
    final int next = counts.length - 1;

    // If new min count is not greater then the next count, store and return the new min count.
    if (newCount <= counts[next]) {
      minCount = newCount;
      return newCount;
    }

    // New min count is greater than the next count, demote the next-to-min element to min and
    // promote the old min element.
    final long error = minError;
    minCount = counts[next];
    minError = errors[next];
    minElement = elements[next];
    indices.remove(minElement);
    return promote(element, newCount, next, error);
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
    if (i == elements.length) {
      return (T) minElement;
    }
    return (T) elements[i];
  }


  public long count(final int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException();
    }
    if (i == elements.length) {
      return minCount;
    }
    return counts[i];
  }

  public long error(final int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException();
    }
    if (i == elements.length) {
      return minError;
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
