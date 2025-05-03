package rxjava;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ObservableTest {

    @AfterEach
    void tearDown() {
        // Gracefully shutdown schedulers after each test
        Schedulers.shutdownGracefully(500, TimeUnit.MILLISECONDS);
    }

    private Disposable createTestDisposable() {
        return new Disposable() {
            private boolean disposed = false;
            
            @Override
            public void dispose() {
                disposed = true;
            }
            
            @Override
            public boolean isDisposed() {
                return disposed;
            }
        };
    }

    @Test
    void testCreate() throws InterruptedException {
        // Given
        List<Integer> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean(false);

        // When
        Observable<Integer> observable = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
            return createTestDisposable();
        });

        observable.subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                received.add(item);
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }

            @Override
            public void onComplete() {
                completed.set(true);
                latch.countDown();
            }
        });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(received).containsExactly(1, 2, 3);
        assertThat(completed.get()).isTrue();
    }

    @Test
    void testMap() throws InterruptedException {
        // Given
        List<String> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        // When
        Observable<Integer> observable = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
            return createTestDisposable();
        });

        observable
                .map(i -> "Number: " + i)
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(received).containsExactly(
                "Number: 1",
                "Number: 2",
                "Number: 3"
        );
    }

    @Test
    void testFilter() throws InterruptedException {
        // Given
        List<Integer> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        // When
        Observable<Integer> observable = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onNext(4);
            observer.onComplete();
            return createTestDisposable();
        });

        observable
                .filter(i -> i % 2 == 0)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(received).containsExactly(2, 4);
    }

    @Test
    void testFlatMap() throws InterruptedException {
        // Given
        List<String> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        // When
        Observable<Integer> observable = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onComplete();
            return createTestDisposable();
        });

        observable
                .flatMap(i -> Observable.<String>create(innerObserver -> {
                    innerObserver.onNext("Value: " + i);
                    innerObserver.onNext("Double: " + (i * 2));
                    innerObserver.onComplete();
                    return createTestDisposable();
                }))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(received).contains(
                "Value: 1",
                "Double: 2",
                "Value: 2",
                "Double: 4"
        );
    }

    @Test
    void testSubscribeOnAndObserveOn() throws InterruptedException {
        // Given
        String mainThread = Thread.currentThread().getName();
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        AtomicReference<String> observeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        // When
        Observable<String> observable = Observable.create(observer -> {
            subscribeThread.set(Thread.currentThread().getName());
            observer.onNext("Test");
            observer.onComplete();
            return createTestDisposable();
        });

        observable
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.computation())
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        observeThread.set(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(subscribeThread.get()).isNotNull();
        assertThat(observeThread.get()).isNotNull();
        assertThat(subscribeThread.get()).isNotEqualTo(mainThread);
        assertThat(observeThread.get()).isNotEqualTo(mainThread);
    }

    @Test
    void testErrorHandling() throws InterruptedException {
        // Given
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        // When
        Observable<Integer> observable = Observable.create(observer -> {
            observer.onNext(1);
            throw new RuntimeException("Test error");
        });

        observable.subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                // Ignore
            }

            @Override
            public void onError(Throwable t) {
                error.set(t);
                latch.countDown();
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(error.get()).isInstanceOf(RuntimeException.class);
        assertThat(error.get().getMessage()).isEqualTo("Test error");
    }

    @Test
    void testDispose() throws InterruptedException {
        // Given
        List<Integer> received = new ArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(1);

        // When
        Observable<Integer> observable = Observable.create(observer -> {
            AtomicBoolean disposed = new AtomicBoolean(false);
            
            new Thread(() -> {
                try {
                    startLatch.countDown();
                    for (int i = 0; i < 100; i++) {
                        if (disposed.get()) break;
                        Thread.sleep(10);
                        observer.onNext(i);
                    }
                    if (!disposed.get()) {
                        observer.onComplete();
                    }
                } catch (Exception e) {
                    if (!disposed.get()) {
                        observer.onError(e);
                    }
                } finally {
                    finishLatch.countDown();
                }
            }).start();
            
            return new Disposable() {
                @Override
                public void dispose() {
                    disposed.set(true);
                }
                
                @Override
                public boolean isDisposed() {
                    return disposed.get();
                }
            };
        });

        Disposable disposable = observable.subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                received.add(item);
            }

            @Override
            public void onError(Throwable t) {
                // Ignore
            }

            @Override
            public void onComplete() {
                // Ignore
            }
        });

        // Ждем, пока поток начнет работу
        startLatch.await(1, TimeUnit.SECONDS);
        // Ждем, пока придут первые значения
        Thread.sleep(100);
        // Отменяем подписку
        disposable.dispose();
        // Ждем, пока поток завершится
        finishLatch.await(2, TimeUnit.SECONDS);

        // Then
        int receivedCount = received.size();
        assertThat(receivedCount).isGreaterThan(0);
        assertThat(receivedCount).isLessThan(100);
    }
    
    @Test
    void testWithSchedulers() throws InterruptedException {
        // Given
        List<String> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        // When
        Observable<Integer> observable = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
            return createTestDisposable();
        });

        observable
                .subscribeOn(Schedulers.io())
                .map(i -> "Mapped: " + i)
                .observeOn(Schedulers.computation())
                .filter(s -> s.contains("2") || s.contains("3"))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(received).containsOnly("Mapped: 2", "Mapped: 3");
    }
} 