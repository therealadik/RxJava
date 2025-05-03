package rxjava;

/**
 * Интерфейс для управления подписками
 */
public interface Disposable {
    /**
     * Отменяет подписку
     */
    void dispose();

    /**
     * Проверяет, была ли отменена подписка
     * @return true, если подписка была отменена
     */
    boolean isDisposed();
} 