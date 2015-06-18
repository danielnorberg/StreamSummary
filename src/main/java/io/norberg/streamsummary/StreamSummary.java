package io.norberg.streamsummary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class StreamSummary<T> {


  private static final class Counter<T> {

    private T element;
    private long count;
    private long error;
    private Counter<T> prev;
    private Counter<T> next;

    public Counter(final T element, final long count, final long error) {
      this.element = element;
      this.count = count;
      this.error = error;
    }

    public Counter(final T element) {
      this(element, 1, 0);
    }

    @Override
    public String toString() {
      return "Counter(" + element + ", " + count + ", " + error + ")";
    }
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

  public long inc(final T element) {

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
    counter = head;
    counters.remove(counter.element);
    counters.put(element, counter);
    counter.element = element;
    counter.error = counter.count;
    counter.count++;

    if (counter.prev != null && counter.count > counter.prev.count) {
      remove(counter);
      insert(counter, counter.prev);
    }

    return counter.count;
  }

  private void insert(final Counter<T> counter, Counter<T> prev) {
    Counter<T> next = prev;
    while (prev != null && counter.count > prev.count) {
      next = prev;
      prev = prev.prev;
    }
    counter.prev = prev;
    counter.next = next;
    next.prev = counter;
    if (prev == null) {
      head = counter;
    } else {
      prev.next = counter;
    }
  }

  private void remove(final Counter<T> counter) {
    if (counter == head) {
      head = counter.next;
    } else {
      counter.prev.next = counter.next;
    }
    if (counter == tail) {
      tail = counter.prev;
    } else {
      counter.next.prev = counter.prev;
    }
  }

  public T element(final int i) {
    return counter(i).element;
  }

  public long count(final int i) {
    return counter(i).count;
  }

  public long error(final int i) {
    return counter(i).error;
  }

  private Counter<T> counter(final int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException();
    }
    Counter<T> counter = head;
    for (int j = 0; j < i; j++) {
      counter = counter.next;
    }
    return counter;
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

  public int enumerate() {
    int n = 0;
    Counter<T> counter = head;
    while (counter != null) {
      n++;
      counter = counter.next;
    }
    return n;
  }
}
