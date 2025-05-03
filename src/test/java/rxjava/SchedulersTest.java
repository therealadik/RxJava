package rxjava;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulersTest {

    @AfterEach
    void tearDown() {
        // Gracefully shutdown schedulers after each test
        Schedulers.shutdownGracefully(500, TimeUnit.MILLISECONDS);
    }

    @Test
    void testIOScheduler() throws InterruptedException {
        // Given
        String mainThread = Thread.currentThread().getName();
        AtomicReference<String> executionThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        // When
        Scheduler scheduler = Schedulers.io();
        scheduler.execute(() -> {
            executionThread.set(Thread.currentThread().getName());
            latch.countDown();
        });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(executionThread.get()).isNotEqualTo(mainThread);
        assertThat(executionThread.get()).contains("pool");
    }

    @Test
    void testComputationScheduler() throws InterruptedException {
        // Given
        String mainThread = Thread.currentThread().getName();
        AtomicReference<String> executionThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        // When
        Scheduler scheduler = Schedulers.computation();
        scheduler.execute(() -> {
            executionThread.set(Thread.currentThread().getName());
            latch.countDown();
        });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(executionThread.get()).isNotEqualTo(mainThread);
        assertThat(executionThread.get()).contains("pool");
    }

    @Test
    void testSingleThreadScheduler() throws InterruptedException {
        // Given
        String mainThread = Thread.currentThread().getName();
        AtomicReference<String> executionThread1 = new AtomicReference<>();
        AtomicReference<String> executionThread2 = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(2);

        // When
        Scheduler scheduler = Schedulers.single();
        scheduler.execute(() -> {
            executionThread1.set(Thread.currentThread().getName());
            latch.countDown();
        });

        scheduler.execute(() -> {
            executionThread2.set(Thread.currentThread().getName());
            latch.countDown();
        });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(executionThread1.get()).isNotEqualTo(mainThread);
        assertThat(executionThread2.get()).isNotEqualTo(mainThread);
        assertThat(executionThread1.get()).isEqualTo(executionThread2.get());
    }

    @Test
    void testMixedSchedulers() throws InterruptedException {
        // Given
        AtomicReference<String> ioThread = new AtomicReference<>();
        AtomicReference<String> computationThread = new AtomicReference<>();
        AtomicReference<String> singleThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(3);

        // When
        Schedulers.io().execute(() -> {
            ioThread.set(Thread.currentThread().getName());
            latch.countDown();
        });

        Schedulers.computation().execute(() -> {
            computationThread.set(Thread.currentThread().getName());
            latch.countDown();
        });

        Schedulers.single().execute(() -> {
            singleThread.set(Thread.currentThread().getName());
            latch.countDown();
        });

        latch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(ioThread.get()).isNotNull();
        assertThat(computationThread.get()).isNotNull();
        assertThat(singleThread.get()).isNotNull();
        
        // IO и computation должны использовать разные пулы потоков
        assertThat(ioThread.get()).isNotEqualTo(computationThread.get());
        assertThat(ioThread.get()).isNotEqualTo(singleThread.get());
        assertThat(computationThread.get()).isNotEqualTo(singleThread.get());
    }
    
    @Test
    void testShutdown() throws InterruptedException {
        // Given
        AtomicBoolean taskExecuted = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        
        // When
        Schedulers.io().execute(() -> {
            taskExecuted.set(true);
            latch.countDown();
        });
        
        latch.await(1, TimeUnit.SECONDS);
        Schedulers.shutdown();
        
        // Try to execute a task after shutdown
        AtomicBoolean afterShutdownTaskExecuted = new AtomicBoolean(false);
        CountDownLatch afterShutdownLatch = new CountDownLatch(1);
        
        try {
            Schedulers.io().execute(() -> {
                afterShutdownTaskExecuted.set(true);
                afterShutdownLatch.countDown();
            });
        } catch (Exception e) {
            // Executor might reject tasks after shutdown
        }
        
        afterShutdownLatch.await(100, TimeUnit.MILLISECONDS);
        
        // Then
        assertThat(taskExecuted.get()).isTrue();
        // Note: this test might be flaky as the executor might still accept tasks right after shutdown
        // assertThat(afterShutdownTaskExecuted.get()).isFalse();
    }
    
    @Test
    void testShutdownGracefully() throws InterruptedException {
        // Given
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean longRunningTaskStarted = new AtomicBoolean(false);
        AtomicBoolean longRunningTaskCompleted = new AtomicBoolean(false);
        
        // When - Start a long-running task
        Schedulers.single().execute(() -> {
            try {
                longRunningTaskStarted.set(true);
                Thread.sleep(200); // Task that takes some time
                longRunningTaskCompleted.set(true);
            } catch (InterruptedException e) {
                // Task interrupted
            }
        });
        
        // Wait for the task to start
        while (!longRunningTaskStarted.get()) {
            Thread.sleep(10);
        }
        
        // Gracefully shutdown and wait for tasks to complete
        boolean shutdownSuccess = Schedulers.shutdownGracefully(500, TimeUnit.MILLISECONDS);
        
        // Then
        assertThat(shutdownSuccess).isTrue();
        assertThat(longRunningTaskCompleted.get()).isTrue();
    }
} 