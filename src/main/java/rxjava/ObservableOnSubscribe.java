package rxjava;

/**
 * Функциональный интерфейс для создания источника данных
 * @param <T> тип данных в потоке
 */
@FunctionalInterface
public interface ObservableOnSubscribe<T> {
    /**
     * Подписывает Observer на источник данных
     * @param observer наблюдатель
     * @return объект Disposable для управления подпиской
     * @throws Throwable в случае ошибки
     */
    Disposable subscribe(Observer<T> observer) throws Throwable;
} 