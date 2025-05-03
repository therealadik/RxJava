package rxjava;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Demonstration of RxJava library implementation
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Testing our RxJava implementation");

        // Creating a counter to wait for asynchronous operations completion
        CountDownLatch latch = new CountDownLatch(1);

        // Example of using Observable with map and filter operators
        Observable<Integer> observable = Observable.create(observer -> {
            System.out.println("Starting number generation");
            try {
                for (int i = 0; i < 10; i++) {
                    System.out.println("Generating number: " + i);
                    observer.onNext(i);
                    Thread.sleep(100); // Simulating work
                }
                observer.onComplete();
            } catch (Exception e) {
                observer.onError(e);
            }
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
        });

        System.out.println("Subscribing to number stream with filtering and transformation");

        // Using map and filter operators
        observable
                .subscribeOn(Schedulers.io())  // Execute subscription in IO thread
                .filter(i -> i % 2 == 0)       // Filtering even numbers
                .map(i -> "Number: " + i)      // Transforming numbers to strings
                .observeOn(Schedulers.computation())  // Processing in computation thread
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        System.out.println("Received: " + item + " in thread: " + Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Error: " + t.getMessage());
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("Stream completed");
                        latch.countDown();
                    }
                });

        System.out.println("Main thread continues working...");

        // Waiting for asynchronous operations to complete
        latch.await(5, TimeUnit.SECONDS);
        System.out.println("Program completed");
        
        // Gracefully shutdown schedulers to allow program to exit
        System.out.println("Shutting down schedulers...");
        boolean shutdownSuccess = Schedulers.shutdownGracefully(1, TimeUnit.SECONDS);
        System.out.println("All schedulers shutdown successfully: " + shutdownSuccess);
    }
}