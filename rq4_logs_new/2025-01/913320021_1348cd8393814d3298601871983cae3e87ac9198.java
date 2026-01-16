package com.management.restaurant.services.observer;
import com.management.restaurant.services.interfaces.Observable;
import com.management.restaurant.services.interfaces.Observer;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Component public class ObserverManager<T> implements Observable<T> {
  private final List<Observer<T>> observers = new ArrayList<>();

  @Override
  public void addObserver(Observer<T> observer) {
    if (!observers.contains(observer)) {
      observers.add(observer);
    }
  }

  @Override
  public void removeObserver(Observer<T> observer) {
    observers.remove(observer);
  }

  @Override
  public void notifyObservers(T data) {
    observers.forEach(observer -> {
      if (data instanceof Observer) {
        observer.updateObserver(data);
      }
    });
  }
  public List<Observer<T>> getObservers() {
    return new ArrayList<>(observers);
  }
}