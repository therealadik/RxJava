package rxjava;

/**
 * Базовый интерфейс Observer для реактивных потоков
 * @param <T> тип данных, передаваемых через поток
 */
public interface Observer<T> {
    /**
     * Получает элементы потока
     * @param item элемент потока
     */
    void onNext(T item);

    /**
     * Обрабатывает ошибки
     * @param t исключение
     */
    void onError(Throwable t);

    /**
     * Вызывается при завершении потока
     */
    void onComplete();
} 