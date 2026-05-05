# BigData
6402 Шелудько Полина 

# Лабораторная 0. MapReduce модель обработки данных

В вычислимом документе MapReduceExamples.ipynb поэтапно развивается модель MapReduce на языке Python. После каждого этапа приводятся примеры задач, сформулированных в рамках полученной модели. В конце документа вам предлагается решить ряд задач из книги Rajaraman A., Ullman J. D. Mining of massive datasets. – Cambridge University Press, 2011.

https://colab.research.google.com/drive/1fzRkZeIVtpX0hKqEXymUDnFi0Z6i1lZJ

Для выполнения кода в Google Colab вам понадобится сделать копию документа под своим google аккаунтом. 


# Лабораторная 1. Решите следующие задачи для данных велопарковок Сан-Франциско (trips.csv, stations.csv):

1.	Найти велосипед с максимальным временем пробега.
2.	Найти наибольшее геодезическое расстояние между станциями.
3.	Найти путь велосипеда с максимальным временем пробега через станции.
4.	Найти количество велосипедов в системе.
5.	Найти пользователей потративших на поездки более 3 часов.

# Лабораторная 2. Формирование отчётов в Apache Spark

## Задание

Сформировать отчёт с информацией о 10 наиболее популярных языках программирования по итогам года за период с 2010 по 2020 годы. Отчёт будет отражать динамику изменения популярности языков программирования и представлять собой набор таблиц "топ-10" для каждого года.

Получившийся отчёт сохранить в формате Apache Parquet.

Для выполнения задания вы можете использовать любую комбинацию Spark API: **RDD API**, **Dataset API**, **SQL API**. 

## Набор данных

Архивы сайтов **Stack Exchange** доступны по адресу https://archive.org/details/stackexchange.

В папке `data` данного репозитория вам доступны:
- выборка данных `posts_sample.xml` (из stackoverflow.com-Posts.7z),
- файл со списком языков `programming-languages.csv`, собранных с вики-страницы https://en.wikipedia.org/wiki/List_of_programming_languages.

Рекомендуется отлаживать решение на небольшой выборке данных `posts_sample.xml`. 


# Лабораторная 3. Потоковая обработка в Apache Flink

В этой лабораторной вы будете работать с [Apache Flink](https://flink.apache.org/) - фреймворком и движком распределённой обработки потоков данных.

## Задание

Выполнить следующие задания из набора заданий репозитория https://github.com/ververica/flink-training-exercises:
  - RideCleanisingExercise
  - RidesAndFaresExercise
  - HourlyTipsExerxise
  - ExpiringStateExercise

Решения могут быть выполнены на двух языках: **Java** или **Scala**. Каждому заданию соответствует `.java` или `.scala` файл с шаблоном решения и файл с тестом решения.  Тесты расположены в папке `test`.

Для выполнения заданий вам потребуется датасет с данными о поездках такси в Нью-Йорке https://github.com/apache/flink-training/blob/master/README.md#using-the-taxi-data-streams. Файлы `nycTaxiFares.gz` и `nycTaxiRides.gz` вы можете найти в папке `data` https://gitlab.com/ssau.tk.courses/big_data/-/tree/master/data. 

## Начало работы

1. git clone https://github.com/ververica/flink-training-exercises
2. Откройте проект в IntelliJ IDEA 
3. Перед выполнением заданий укажите путь к данным в переменных `pathToRideData` и `pathToFareData` в файле `./flink-training-exercises/src/main/java/com/ververica/flinktraining/exercises/datastream_java/utils/ExerciseBase.java`.
4. Для выполнения первого задания на **Scala** откройте файл `./flink-training-exercises/src/main/scala/com/ververica/flinktraining/exercises/datastream_scala/basics/RideCleansingExercise.scala`. В месте решения вы найдёте `throw new MissingSolutionException()`.
5. Запустите тест `./flink-training-exercises/src/test/java/com/ververica/flinktraining/exercises/datastream_java/basics/RideCleansingScalaTest.java`. Тест должен завершиться успешно, но сама программа будет завершаться аварийно с исключением `MissingSolutionException`.
6. Реализуйте недостающий код. С помощью теста проверьте корректность работы вашего решения.


Зарегистрируйтесь на сайте https://training.ververica.com для доступа к теоретическим материалам и дополнительным упражнениям.
