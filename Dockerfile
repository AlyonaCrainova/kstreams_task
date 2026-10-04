# ЭТАП 1: Сборка проекта
FROM maven:3.8-openjdk-17 AS builder
WORKDIR /app

# Копируем файлы проекта
COPY pom.xml .
COPY src ./src

# Запускаем сборку.
# Плагин assembly создаст файл с суффиксом -jar-with-dependencies.jar
RUN mvn clean package -DskipTests

# ЭТАП 2: Запуск приложения
FROM eclipse-temurin:17-jre
WORKDIR /app

# КОПИРУЕМ ТОЛЬКО ОДИН БОЛЬШОЙ JAR-ФАЙЛ
# Путь слева: где файл лежит после сборки (в этапе builder)
# Путь справа: куда мы его кладем для запуска
COPY --from=builder /app/target/kstreams_task-1.0-SNAPSHOT-jar-with-dependencies.jar /app/app.jar