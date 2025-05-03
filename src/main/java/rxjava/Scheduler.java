package rxjava;

/**
 * Интерфейс для управления потоками выполнения
 */
public interface Scheduler {
    /**
     * Выполняет задачу в соответствующем потоке
     * @param task задача для выполнения
     */
    void execute(Runnable task);
} 