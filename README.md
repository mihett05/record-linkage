# record-linkage

Решение задачи Record Linkage

## Структура

- groups/ - группировка в CH
- preprocess/ - пайплайны для обработки данных из входных датасетов

## Решение

1. Нормализуем и объединяем данные из трёх датасетов
2. Кластерезируем и обрабатываем строки, реализуя Incremental Record Linkage
3. Сохраняем в итоговый результат

## Как запустить?

1. `docker build -t linkage`
2. `docker run -e CH_HOST=<адрес clickhouse (clickhouse)> -e CH_PORT=<порт clickhouse (8123)> linkage`
