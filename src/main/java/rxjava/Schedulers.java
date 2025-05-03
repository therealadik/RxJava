package rxjava;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Фабрика планировщиков потоков
 */
public class Schedulers {
    private static final AtomicReference<IOThreadScheduler> IO_SCHEDULER = 
            new AtomicReference<>(new IOThreadScheduler());
    private static final AtomicReference<ComputationScheduler> COMPUTATION_SCHEDULER = 
            new AtomicReference<>(new ComputationScheduler());
    private static final AtomicReference<SingleThreadScheduler> SINGLE_THREAD_SCHEDULER = 
            new AtomicReference<>(new SingleThreadScheduler());

    private Schedulers() {
        // Запрещаем создание экземпляров
    }

    /**
     * Возвращает планировщик для операций ввода-вывода
     * @return IOThreadScheduler
     */
    public static Scheduler io() {
        IOThreadScheduler scheduler = IO_SCHEDULER.get();
        if (scheduler == null || scheduler.isShutdown()) {
            IOThreadScheduler newScheduler = new IOThreadScheduler();
            IO_SCHEDULER.compareAndSet(scheduler, newScheduler);
            scheduler = IO_SCHEDULER.get();
        }
        return scheduler;
    }

    /**
     * Возвращает планировщик для вычислительных операций
     * @return ComputationScheduler
     */
    public static Scheduler computation() {
        ComputationScheduler scheduler = COMPUTATION_SCHEDULER.get();
        if (scheduler == null || scheduler.isShutdown()) {
            ComputationScheduler newScheduler = new ComputationScheduler();
            COMPUTATION_SCHEDULER.compareAndSet(scheduler, newScheduler);
            scheduler = COMPUTATION_SCHEDULER.get();
        }
        return scheduler;
    }

    /**
     * Возвращает планировщик с одним потоком
     * @return SingleThreadScheduler
     */
    public static Scheduler single() {
        SingleThreadScheduler scheduler = SINGLE_THREAD_SCHEDULER.get();
        if (scheduler == null || scheduler.isShutdown()) {
            SingleThreadScheduler newScheduler = new SingleThreadScheduler();
            SINGLE_THREAD_SCHEDULER.compareAndSet(scheduler, newScheduler);
            scheduler = SINGLE_THREAD_SCHEDULER.get();
        }
        return scheduler;
    }
    
    /**
     * Завершает работу всех планировщиков
     */
    public static void shutdown() {
        IOThreadScheduler ioScheduler = IO_SCHEDULER.getAndSet(null);
        if (ioScheduler != null) {
            ioScheduler.shutdown();
        }
        
        ComputationScheduler computationScheduler = COMPUTATION_SCHEDULER.getAndSet(null);
        if (computationScheduler != null) {
            computationScheduler.shutdown();
        }
        
        SingleThreadScheduler singleThreadScheduler = SINGLE_THREAD_SCHEDULER.getAndSet(null);
        if (singleThreadScheduler != null) {
            singleThreadScheduler.shutdown();
        }
    }
    
    /**
     * Завершает работу всех планировщиков с ожиданием завершения задач
     * @param timeout время ожидания
     * @param unit единица измерения времени
     * @return true, если все планировщики завершились корректно
     */
    public static boolean shutdownGracefully(long timeout, TimeUnit unit) {
        boolean ioShutdown = true;
        boolean computationShutdown = true;
        boolean singleShutdown = true;
        
        IOThreadScheduler ioScheduler = IO_SCHEDULER.getAndSet(null);
        if (ioScheduler != null) {
            ioShutdown = ioScheduler.shutdownGracefully(timeout, unit);
        }
        
        ComputationScheduler computationScheduler = COMPUTATION_SCHEDULER.getAndSet(null);
        if (computationScheduler != null) {
            computationShutdown = computationScheduler.shutdownGracefully(timeout, unit);
        }
        
        SingleThreadScheduler singleThreadScheduler = SINGLE_THREAD_SCHEDULER.getAndSet(null);
        if (singleThreadScheduler != null) {
            singleShutdown = singleThreadScheduler.shutdownGracefully(timeout, unit);
        }
        
        return ioShutdown && computationShutdown && singleShutdown;
    }

    /**
     * Планировщик для операций ввода-вывода (использует CachedThreadPool)
     */
    static class IOThreadScheduler implements Scheduler {
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private volatile boolean shutdown = false;

        @Override
        public void execute(Runnable task) {
            if (shutdown) {
                throw new IllegalStateException("Scheduler has been shut down");
            }
            executor.execute(task);
        }
        
        /**
         * Проверяет, был ли планировщик остановлен
         * @return true, если планировщик был остановлен
         */
        public boolean isShutdown() {
            return shutdown || executor.isShutdown();
        }
        
        /**
         * Завершает работу планировщика
         */
        public void shutdown() {
            shutdown = true;
            executor.shutdown();
        }
        
        /**
         * Завершает работу планировщика с ожиданием завершения задач
         * @param timeout время ожидания
         * @param unit единица измерения времени
         * @return true, если планировщик завершился корректно
         */
        public boolean shutdownGracefully(long timeout, TimeUnit unit) {
            shutdown = true;
            executor.shutdown();
            try {
                return executor.awaitTermination(timeout, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * Планировщик для вычислительных операций (использует FixedThreadPool)
     */
    static class ComputationScheduler implements Scheduler {
        private final ExecutorService executor = Executors.newFixedThreadPool(
                Math.max(1, Runtime.getRuntime().availableProcessors())
        );
        private volatile boolean shutdown = false;

        @Override
        public void execute(Runnable task) {
            if (shutdown) {
                throw new IllegalStateException("Scheduler has been shut down");
            }
            executor.execute(task);
        }
        
        /**
         * Проверяет, был ли планировщик остановлен
         * @return true, если планировщик был остановлен
         */
        public boolean isShutdown() {
            return shutdown || executor.isShutdown();
        }
        
        /**
         * Завершает работу планировщика
         */
        public void shutdown() {
            shutdown = true;
            executor.shutdown();
        }
        
        /**
         * Завершает работу планировщика с ожиданием завершения задач
         * @param timeout время ожидания
         * @param unit единица измерения времени
         * @return true, если планировщик завершился корректно
         */
        public boolean shutdownGracefully(long timeout, TimeUnit unit) {
            shutdown = true;
            executor.shutdown();
            try {
                return executor.awaitTermination(timeout, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /**
     * Планировщик с одним потоком (использует один поток)
     */
    static class SingleThreadScheduler implements Scheduler {
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private volatile boolean shutdown = false;

        @Override
        public void execute(Runnable task) {
            if (shutdown) {
                throw new IllegalStateException("Scheduler has been shut down");
            }
            executor.execute(task);
        }
        
        /**
         * Проверяет, был ли планировщик остановлен
         * @return true, если планировщик был остановлен
         */
        public boolean isShutdown() {
            return shutdown || executor.isShutdown();
        }
        
        /**
         * Завершает работу планировщика
         */
        public void shutdown() {
            shutdown = true;
            executor.shutdown();
        }
        
        /**
         * Завершает работу планировщика с ожиданием завершения задач
         * @param timeout время ожидания
         * @param unit единица измерения времени
         * @return true, если планировщик завершился корректно
         */
        public boolean shutdownGracefully(long timeout, TimeUnit unit) {
            shutdown = true;
            executor.shutdown();
            try {
                return executor.awaitTermination(timeout, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }
} 