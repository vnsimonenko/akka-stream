Есть файл на 100000 строк, в нем записи в формате "ID;amount";
Всего уникальных ID в файле 1000 штук.
Используя Akka необходимо просуммировать все операции по каждому ID
и записать агрегированный результат в отдельный файл.