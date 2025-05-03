# Отчет о реализации библиотеки RxJava

## Архитектура системы

Реализованная система представляет собой упрощенную версию библиотеки RxJava и включает следующие компоненты:

### Базовые интерфейсы

1. **Observer<T>** - интерфейс, представляющий наблюдателя, который получает уведомления о событиях:
   - `onNext(T item)` - получает элементы потока
   - `onError(Throwable t)` - обрабатывает ошибки
   - `onComplete()` - вызывается при завершении потока

2. **ObservableOnSubscribe<T>** - функциональный интерфейс, представляющий источник данных:
   - `void subscribe(Observer<T> observer)` - подписывает наблюдателя на источник данных

3. **Disposable** - интерфейс для управления подписками:
   - `void dispose()` - отменяет подписку
   - `boolean isDisposed()` - проверяет, была ли отменена подписка

4. **Scheduler** - интерфейс для управления потоками выполнения:
   - `void execute(Runnable task)` - выполняет задачу в соответствующем потоке

### Основные классы

1. **Observable<T>** - основной класс для создания и управления реактивными потоками:
   - `static <T> Observable<T> create(ObservableOnSubscribe<T> source)` - создает новый Observable
   - `Disposable subscribe(Observer<T> observer)` - подписывает наблюдателя на поток
   - `Observable<T> subscribeOn(Scheduler scheduler)` - задает планировщик для выполнения подписки
   - `Observable<T> observeOn(Scheduler scheduler)` - задает планировщик для обработки элементов
   - `<R> Observable<R> map(Function<T, R> mapper)` - преобразует элементы потока
   - `Observable<T> filter(Predicate<T> predicate)` - фильтрует элементы потока
   - `<R> Observable<R> flatMap(Function<T, Observable<R>> mapper)` - преобразует элементы потока в новые Observable и объединяет результаты

2. **Schedulers** - фабричный класс для получения различных планировщиков:
   - `static Scheduler io()` - возвращает планировщик для операций ввода-вывода
   - `static Scheduler computation()` - возвращает планировщик для вычислительных операций
   - `static Scheduler single()` - возвращает планировщик с одним потоком

## Принципы работы Schedulers

В нашей реализации мы создали три типа планировщиков:

1. **IOThreadScheduler** - использует `Executors.newCachedThreadPool()`. Этот планировщик создает новые потоки по мере необходимости и переиспользует ранее созданные потоки, если они доступны. Оптимален для задач ввода-вывода (I/O), где потоки могут проводить большую часть времени в состоянии ожидания.

2. **ComputationScheduler** - использует `Executors.newFixedThreadPool(n)`, где n - количество доступных процессоров. Этот планировщик создает фиксированное количество потоков, равное количеству процессоров, что оптимально для CPU-интенсивных вычислительных задач.

3. **SingleThreadScheduler** - использует `Executors.newSingleThreadExecutor()`. Этот планировщик гарантирует, что все задачи будут выполняться последовательно в одном потоке, что полезно для задач, требующих строгого порядка выполнения.

### Области применения планировщиков

- **IOThreadScheduler (Schedulers.io())** - подходит для операций с базой данных, файловых операций, сетевых запросов и других задач ввода-вывода.

- **ComputationScheduler (Schedulers.computation())** - оптимален для CPU-интенсивных операций, таких как математические вычисления, обработка изображений, криптографические операции и т.п.

- **SingleThreadScheduler (Schedulers.single())** - используется, когда необходимо гарантировать последовательность выполнения операций, например при работе с некоторыми API, требующими вызова в определенном потоке (UI-поток в Android).

### Различия между subscribeOn и observeOn

- **subscribeOn** - определяет, в каком потоке будет выполняться вся цепочка операций, начиная с создания Observable. Если в цепочке есть несколько вызовов subscribeOn, действует только первый.

- **observeOn** - определяет, в каком потоке будут выполняться последующие операции в цепочке. Можно использовать несколько вызовов observeOn для переключения между потоками на разных этапах обработки.

## Тестирование

Для тестирования реализации были разработаны юнит-тесты, охватывающие все основные компоненты системы:

1. **ObservableTest** - тесты для класса Observable и его операторов:
   - `testCreate()` - проверка создания и работы Observable
   - `testMap()` - проверка работы оператора map
   - `testFilter()` - проверка работы оператора filter
   - `testFlatMap()` - проверка работы оператора flatMap
   - `testSubscribeOnAndObserveOn()` - проверка работы методов subscribeOn и observeOn
   - `testErrorHandling()` - проверка обработки ошибок
   - `testDispose()` - проверка отмены подписки через Disposable

2. **SchedulersTest** - тесты для планировщиков:
   - `testIOScheduler()` - проверка работы планировщика io
   - `testComputationScheduler()` - проверка работы планировщика computation
   - `testSingleThreadScheduler()` - проверка работы планировщика single
   - `testMixedSchedulers()` - проверка взаимодействия разных планировщиков

## Примеры использования

### Пример 1: Базовая подписка

```java
Observable<Integer> observable = Observable.create(observer -> {
    observer.onNext(1);
    observer.onNext(2);
    observer.onNext(3);
    observer.onComplete();
});

observable.subscribe(new Observer<Integer>() {
    @Override
    public void onNext(Integer item) {
        System.out.println("Получено: " + item);
    }

    @Override
    public void onError(Throwable t) {
        System.err.println("Ошибка: " + t.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("Завершено");
    }
});
```

### Пример 2: Использование операторов map и filter

```java
Observable<Integer> observable = Observable.create(observer -> {
    for (int i = 0; i < 10; i++) {
        observer.onNext(i);
    }
    observer.onComplete();
});

observable
    .filter(i -> i % 2 == 0)       // Оставляем только четные числа
    .map(i -> "Число: " + i)       // Преобразуем в строки
    .subscribe(new Observer<String>() {
        @Override
        public void onNext(String item) {
            System.out.println(item);
        }

        @Override
        public void onError(Throwable t) {
            System.err.println("Ошибка: " + t.getMessage());
        }

        @Override
        public void onComplete() {
            System.out.println("Завершено");
        }
    });
```

### Пример 3: Асинхронная обработка с планировщиками

```java
Observable<Integer> observable = Observable.create(observer -> {
    System.out.println("Создание: " + Thread.currentThread().getName());
    for (int i = 0; i < 5; i++) {
        observer.onNext(i);
    }
    observer.onComplete();
});

observable
    .subscribeOn(Schedulers.io())            // Подписка в IO потоке
    .map(i -> {
        System.out.println("Map: " + Thread.currentThread().getName());
        return i * 10;
    })
    .observeOn(Schedulers.computation())     // Переключаемся на computation поток
    .filter(i -> {
        System.out.println("Filter: " + Thread.currentThread().getName());
        return i > 20;
    })
    .subscribe(new Observer<Integer>() {
        @Override
        public void onNext(Integer item) {
            System.out.println("Получено: " + item + " в " + Thread.currentThread().getName());
        }

        @Override
        public void onError(Throwable t) {
            System.err.println("Ошибка: " + t.getMessage());
        }

        @Override
        public void onComplete() {
            System.out.println("Завершено в " + Thread.currentThread().getName());
        }
    });
```

### Пример 4: Использование flatMap для асинхронных запросов

```java
Observable<Integer> ids = Observable.create(observer -> {
    observer.onNext(1);
    observer.onNext(2);
    observer.onNext(3);
    observer.onComplete();
});

ids.flatMap(id -> Observable.create(observer -> {
    // Имитация асинхронного запроса
    new Thread(() -> {
        try {
            Thread.sleep(100);
            observer.onNext("Результат для ID: " + id);
            observer.onComplete();
        } catch (Exception e) {
            observer.onError(e);
        }
    }).start();
}))
.subscribe(new Observer<String>() {
    @Override
    public void onNext(String item) {
        System.out.println(item);
    }

    @Override
    public void onError(Throwable t) {
        System.err.println("Ошибка: " + t.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("Все запросы выполнены");
    }
});
```

## Заключение

В данном проекте была реализована упрощенная версия библиотеки RxJava, включающая базовые компоненты реактивного потока, основные операторы трансформации данных и планировщики для управления потоками выполнения. Созданная реализация соответствует основным концепциям реактивного программирования и может быть использована для обработки асинхронных событий и создания реактивных приложений. 
