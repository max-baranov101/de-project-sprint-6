# Яндекс Практикум. Инженер данных. 6 спринт. Проектная работа по аналитическим базам данных

## Общее описание

- Репозиторий предназначен для сдачи проекта 6-го спринта по по аналитическим базам данных
- Этот проект включает создание и развитие аналитической базы данных Vertica, предназначенной для помощи маркетологам в оптимизации рекламы в социальных сетях.
- Основная цель проекта: расширить модель данных и развить БД Vertica для анализа активности пользователей в сообществах.
- Бизнес-задача: определить сообщества с высокой конверсией, где значительная часть пользователей активно участвует в обсуждениях, что указывает на высокие показатели конверсии первых сообщений. Это необходимо маркетологам для размещения рекламы активных сообществ на сторонних сайтах для привлечения новых пользователей.

## Изучение технологий, которые используются для получения данных из источников

- AWS S3: хранение исходных данных.
- Apache Airflow: оркестровка процесса загрузки данных.
- HP Vertica: хранилище данных для аналитики.
- SQL: манипуляция данными и выполнения запросов.
  
## Изучение данных

1. Источник данных для БД — S3.
2. Данные в S3 лежат в бакетах.
3. Креды для подключения к S3
   - AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
   - AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
4. Описание данных
   - `users.csv`: информация о пользователях
     - `id` — уникальный идентификатор пользователя
     - `сhat_name` — имя/название чата
     - `registration_dt` — дата регистрации пользователя
     - `country` — страна пользователя
     - `age` — возраст
   - `groups.csv`: информация о группах
     - `id` — идентификатор группы пользователей
     - `admin_id` — идентификатор администратора
     - `group_name` — название группы
     - `registration_dt` — дата регистрации группы
     - `is_private` — является ли группа приватной
   - `dialogs.csv`: информация о диалогах
     - `message_id` — это идентификатор сообщения
     - `datetime` — дата и время сообщения
     - `message_from` — идентификатор отправителя сообщения
     - `message_to` — идентификатор получателя
     - `message` — текст сообщения
   - `group_log.csv`: логи групп
     - `group_id` — уникальный идентификатор группы;
     - `user_id` - уникальный идентификатор пользователя.
     - `user_id_from` — поле для отметки о том, что пользователь не сам вступил в группу, а его добавил другой участник. Если пользователя пригласил в группу кто-то другой, поле будет непустым.
     - `event` - действие пользователя (`create`, `add`, `leave`).
     - `datetime` - временная метка события

## Выполнение проекта

### Подготовка DDL-скриптов

1. Создание таблиц STAGING ([1_ddl_stg.sql](src/sql/1_ddl_stg.sql))
2. Расширение STAGING согласно условиям данного проекта: создание таблицы `group_log` с учётом формата данных в скачанном файле `group_log.csv`.
3. Создание таблиц DWH ([3_ddl_dwh.sql](src/sql/3_ddl_dwh.sql))
4. Расширение DWH согласно условиям данного проекта:
   - Создание таблицы связи `l_user_group_activity` ([4_create_l_user_group_activity.sql](src/sql/4_create_l_user_group_activity.sql))
     - `hk_l_user_group_activity` — основной ключ типа `INT`
     - `hk_user_id` — внешний ключ типа `INT`, который связан с основным ключом хаба `h_users`
     - `hk_group_id` — внешний ключ типа `INT`, который связан с основным ключом хаба `h_groups`
     - `load_dt` — временная отметка типа `DATETIME` о том, когда были загружены данные
     - `load_src` — данные об источнике типа `VARCHAR(20)`
   - Создание таблицы сателлита `s_auth_history` ([5_create_s_auth_history.sql](src/sql/5_create_s_auth_history.sql))
     - `hk_l_user_group_activity` — внешний ключ к ранее созданной таблице связей типа `INT`
     - `user_id_from` — идентификатор типа `INT` того пользователя, который добавил в группу другого
     - `event` — событие пользователя в группе;
     - `event_dt` — дата и время, когда совершилось событие;
     - `load_dt` — временная отметка типа `DATETIME` о том, когда были загружены данные
     - `load_src` — данные об источнике типа `VARCHAR(20)`
5. Запуск DDL-скриптов один раз из файла [1_ddl.py](src/1_ddl.py)

### Сбор данных и наполнение таблиц STAGING

1. Создание в Airflow подключения `de_vertica` к Vertica
   - `host`: `vertica.tgcloudenv.ru`
   - `port`: `5433`
   - `user`: `stv202404101`
   - `password`: `D1e67eX3ve3Q0PP`
   - `database`: `dwh`
2. Подготовка DAG по сохранению данных из S3 [s3_download_dag.py](dags/s3_download_dag.py)
3. Подготовка DAG по загрузке данных в Vertica [vertica_upload_dag.py.py](dags/vertica_upload_dag.py)

### Наполнение таблицы связи и саттелита

1. Подготовка скрипта миграции в таблицу связи
2. Подготовка скрипта наполнения сателлита
3. Запуск скриптов один раз из файла [2_insert_data.py](src/2_insert_data.py)

### CTE

### Ответ бизнесу

1. Подготовка комплексного SQL-запроса для ответа на вопрос бизнеса для десяти самых старых групп
   - Хэш-ключ группы `hk_group_id`
   - Количество новых пользователей `cnt_added_users` в таких группах
   - Количество пользователей `cnt_users_in_group_with_messages` в каждой такой группе, которые написали хотя бы одно сообщение
   - Доля пользователей `group_conversion` в каждой такой группе, которые начали общаться
   - Все метрики должны быть отсортированы по убыванию `group_conversion`
2. Запуск SQL-запроса из файла [ddl_full.py](src/ddl_full.py)
3. Результаты запроса
