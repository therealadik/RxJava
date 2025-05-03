package rxjava;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Основной класс для реактивных потоков
 * @param <T> тип данных в потоке
 */
public class Observable<T> {
    private final ObservableOnSubscribe<T> source;
    private Scheduler subscribeOnScheduler;
    private Scheduler observeOnScheduler;

    private Observable(ObservableOnSubscribe<T> source) {
        this.source = source;
    }

    /**
     * Создает новый Observable с указанным источником данных
     * @param source источник данных
     * @param <T> тип данных
     * @return новый Observable
     */
    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        return new Observable<>(source);
    }

    /**
     * Подписывает наблюдателя на поток данных
     * @param observer наблюдатель
     * @return объект Disposable для управления подпиской
     */
    public Disposable subscribe(Observer<T> observer) {
        ObserverWrapper<T> observerWrapper = new ObserverWrapper<>(observer);

        Runnable subscribeTask = () -> {
            try {
                source.subscribe(observerWrapper);
            } catch (Throwable e) {
                observerWrapper.onError(e);
            }
        };

        if (subscribeOnScheduler != null) {
            subscribeOnScheduler.execute(subscribeTask);
        } else {
            subscribeTask.run();
        }

        return observerWrapper;
    }

    /**
     * Задает планировщик для выполнения подписки
     * @param scheduler планировщик
     * @return новый Observable
     */
    public Observable<T> subscribeOn(Scheduler scheduler) {
        Observable<T> observable = new Observable<>(this.source);
        observable.subscribeOnScheduler = scheduler;
        observable.observeOnScheduler = this.observeOnScheduler;
        return observable;
    }

    /**
     * Задает планировщик для обработки элементов
     * @param scheduler планировщик
     * @return новый Observable
     */
    public Observable<T> observeOn(Scheduler scheduler) {
        Observable<T> observable = new Observable<>(this.source);
        observable.subscribeOnScheduler = this.subscribeOnScheduler;
        observable.observeOnScheduler = scheduler;
        return observable;
    }

    /**
     * Преобразует элементы потока с помощью функции маппера
     * @param mapper функция преобразования
     * @param <R> тип результата
     * @return новый Observable с преобразованными элементами
     */
    public <R> Observable<R> map(Function<T, R> mapper) {
        return create(observer -> {
            Disposable disposable = this.subscribe(
                new ObserverWithEmitter<>(observer, mapper, observeOnScheduler)
            );
            return disposable;
        });
    }

    /**
     * Фильтрует элементы потока с помощью предиката
     * @param predicate функция фильтрации
     * @return новый Observable с отфильтрованными элементами
     */
    public Observable<T> filter(Predicate<T> predicate) {
        return create(observer -> {
            Disposable disposable = this.subscribe(
                new FilteringObserver<>(observer, predicate, observeOnScheduler)
            );
            return disposable;
        });
    }

    /**
     * Преобразует каждый элемент потока в новый Observable и объединяет результаты
     * @param mapper функция преобразования элемента в Observable
     * @param <R> тип результата
     * @return новый Observable с объединенными результатами
     */
    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return create(observer -> {
            Disposable disposable = this.subscribe(
                new FlatMapObserver<>(observer, mapper, observeOnScheduler)
            );
            return disposable;
        });
    }

    /**
     * Класс-обертка для Observer, реализующий Disposable
     * @param <T> тип данных
     */
    private static class ObserverWrapper<T> implements Observer<T>, Disposable {
        private final Observer<T> actual;
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        public ObserverWrapper(Observer<T> actual) {
            this.actual = actual;
        }

        @Override
        public void onNext(T item) {
            if (!isDisposed()) {
                actual.onNext(item);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (!isDisposed()) {
                try {
                    actual.onError(t);
                } finally {
                    dispose();
                }
            }
        }

        @Override
        public void onComplete() {
            if (!isDisposed()) {
                try {
                    actual.onComplete();
                } finally {
                    dispose();
                }
            }
        }

        @Override
        public void dispose() {
            disposed.set(true);
        }

        @Override
        public boolean isDisposed() {
            return disposed.get();
        }
    }
    
    /**
     * Наблюдатель для оператора map
     * @param <T> тип входных данных
     * @param <R> тип выходных данных
     */
    private static class ObserverWithEmitter<T, R> implements Observer<T> {
        private final Observer<R> emitter;
        private final Function<T, R> mapper;
        private final Scheduler scheduler;

        public ObserverWithEmitter(Observer<R> emitter, Function<T, R> mapper, Scheduler scheduler) {
            this.emitter = emitter;
            this.mapper = mapper;
            this.scheduler = scheduler;
        }

        @Override
        public void onNext(T item) {
            try {
                R mappedItem = mapper.apply(item);
                if (scheduler != null) {
                    scheduler.execute(() -> emitter.onNext(mappedItem));
                } else {
                    emitter.onNext(mappedItem);
                }
            } catch (Throwable e) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (scheduler != null) {
                scheduler.execute(() -> emitter.onError(t));
            } else {
                emitter.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (scheduler != null) {
                scheduler.execute(() -> emitter.onComplete());
            } else {
                emitter.onComplete();
            }
        }
    }
    
    /**
     * Наблюдатель для оператора filter
     * @param <T> тип данных
     */
    private static class FilteringObserver<T> implements Observer<T> {
        private final Observer<T> emitter;
        private final Predicate<T> predicate;
        private final Scheduler scheduler;

        public FilteringObserver(Observer<T> emitter, Predicate<T> predicate, Scheduler scheduler) {
            this.emitter = emitter;
            this.predicate = predicate;
            this.scheduler = scheduler;
        }

        @Override
        public void onNext(T item) {
            try {
                boolean passes = predicate.test(item);
                if (passes) {
                    if (scheduler != null) {
                        scheduler.execute(() -> emitter.onNext(item));
                    } else {
                        emitter.onNext(item);
                    }
                }
            } catch (Throwable e) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (scheduler != null) {
                scheduler.execute(() -> emitter.onError(t));
            } else {
                emitter.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (scheduler != null) {
                scheduler.execute(() -> emitter.onComplete());
            } else {
                emitter.onComplete();
            }
        }
    }
    
    /**
     * Наблюдатель для оператора flatMap
     * @param <T> тип входных данных
     * @param <R> тип выходных данных
     */
    private static class FlatMapObserver<T, R> implements Observer<T> {
        private final Observer<R> emitter;
        private final Function<T, Observable<R>> mapper;
        private final Scheduler scheduler;

        public FlatMapObserver(Observer<R> emitter, Function<T, Observable<R>> mapper, Scheduler scheduler) {
            this.emitter = emitter;
            this.mapper = mapper;
            this.scheduler = scheduler;
        }

        @Override
        public void onNext(T item) {
            try {
                Observable<R> mappedObservable = mapper.apply(item);
                mappedObservable.subscribe(new Observer<R>() {
                    @Override
                    public void onNext(R innerItem) {
                        if (scheduler != null) {
                            scheduler.execute(() -> emitter.onNext(innerItem));
                        } else {
                            emitter.onNext(innerItem);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        FlatMapObserver.this.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        // Внутренний Observable завершился - ничего не делаем
                    }
                });
            } catch (Throwable e) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (scheduler != null) {
                scheduler.execute(() -> emitter.onError(t));
            } else {
                emitter.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (scheduler != null) {
                scheduler.execute(() -> emitter.onComplete());
            } else {
                emitter.onComplete();
            }
        }
    }
} 